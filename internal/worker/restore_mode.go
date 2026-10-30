package worker

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	restoreModeTXID                     = "txid"
	restoreModeTimestamp                = "timestamp"
	restoreModeLatest                   = "latest"
	restoreModeFallbackNoLTXBoundary    = "timestamp_no_ltx_boundary"
	restoreModeFallbackNoMarker         = "latest_marker_unavailable"
	restoreModeFallbackRotationDisabled = "rotation_disabled"
	restoreMarkerTable                  = "_soak_restore_marker"
	restoreMarkerRetained               = 16
	ltxHeaderTimestampEnd               = 40
)

var (
	restoreModeRotation    = []string{restoreModeTXID, restoreModeTimestamp, restoreModeLatest}
	restorePlanTXIDPattern = regexp.MustCompile(`restore plan"?[ ,].*?\btxid"?[=:]"?([0-9a-f]{16})\b`)
)

func restoreModeOffset(profileName string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(profileName))
	return int(hash.Sum32() % uint32(len(restoreModeRotation)))
}

func (v *Verifier) restoreModeAt(at time.Time) string {
	if !v.cfg.VerifyRestoreRotation {
		return restoreModeTXID
	}
	interval := v.cfg.VerifyInterval
	if interval <= 0 {
		interval = DefaultConfig().VerifyInterval
	}
	slot := at.UnixNano() / int64(interval)
	return restoreModeRotation[(int(slot%int64(len(restoreModeRotation)))+restoreModeOffset(v.cfg.ProfileName))%len(restoreModeRotation)]
}

type restoreEvidenceInconclusiveError struct {
	err error
}

func (e *restoreEvidenceInconclusiveError) Error() string {
	return e.err.Error()
}

func (e *restoreEvidenceInconclusiveError) Unwrap() error {
	return e.err
}

// restoreCapacityInconclusiveError marks a restore the harness could not
// complete for lack of disk space at its own output path. It is inconclusive
// for the build under test and carries the harness capacity classification.
type restoreCapacityInconclusiveError struct {
	err         error
	neededBytes uint64
	freeBytes   uint64
}

func (e *restoreCapacityInconclusiveError) Error() string {
	return e.err.Error()
}

func (e *restoreCapacityInconclusiveError) Unwrap() error {
	return e.err
}

func writeRestoreMarker(ctx context.Context, path string, marker int64, txidHint uint64) error {
	defer harnessWriters.begin("verifier_restore_marker", path)()
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw&_pragma=busy_timeout(5000)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	statements := []struct {
		query string
		args  []any
	}{
		{query: "CREATE TABLE IF NOT EXISTS " + restoreMarkerTable + " (cycle INTEGER PRIMARY KEY, txid_hint INTEGER NOT NULL, created_at TEXT NOT NULL)"},
		{query: "INSERT INTO " + restoreMarkerTable + " (cycle, txid_hint, created_at) VALUES (?, ?, ?)", args: []any{marker, int64(txidHint), time.Now().UTC().Format(time.RFC3339Nano)}},
		{query: "DELETE FROM " + restoreMarkerTable + " WHERE cycle NOT IN (SELECT cycle FROM " + restoreMarkerTable + " ORDER BY cycle DESC LIMIT ?)", args: []any{restoreMarkerRetained}},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return fmt.Errorf("write restore marker: %w", err)
		}
	}
	return tx.Commit()
}

func checkRestoreMarker(ctx context.Context, path string, marker int64) error {
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var found int
	err = db.QueryRowContext(ctx, "SELECT 1 FROM "+restoreMarkerTable+" WHERE cycle = ?", marker).Scan(&found)
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
		return fmt.Errorf("restore failed: latest restore behind boundary: marker=%d missing", marker)
	}
	return fmt.Errorf("restore failed: latest restore marker: %w", err)
}

func restoreArgs(configPath, mode string, txid uint64, timestamp time.Time, restoredPath, sourcePath string) []string {
	args := []string{"restore", "-config", configPath}
	switch mode {
	case restoreModeTimestamp:
		args = append(args, "-timestamp", timestamp.UTC().Format(time.RFC3339Nano))
	case restoreModeLatest:
	default:
		args = append(args, "-txid", formatTXID(txid))
	}
	return append(args, "-o", restoredPath, sourcePath)
}

func parseRestorePlanTXID(line string) (uint64, bool) {
	match := restorePlanTXIDPattern.FindStringSubmatch(line)
	if match == nil {
		return 0, false
	}
	txid, err := strconv.ParseUint(match[1], 16, 64)
	return txid, err == nil
}

func boundaryLTXTimestamp(sourcePath string, txid uint64) (time.Time, bool) {
	dir := filepath.Join(litestreamStateDir(sourcePath), "ltx", "0")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return time.Time{}, false
	}
	var boundary time.Time
	var found bool
	var later []time.Time
	for _, entry := range entries {
		minTXID, maxTXID, ok := parseLTXFilename(entry.Name())
		if !ok || (maxTXID != txid && minTXID <= txid) {
			continue
		}
		timestamp, err := readLTXHeaderTimestamp(filepath.Join(dir, entry.Name()), minTXID, maxTXID)
		if err != nil {
			return time.Time{}, false
		}
		if maxTXID == txid {
			boundary, found = timestamp, true
			continue
		}
		later = append(later, timestamp)
	}
	if !found {
		return time.Time{}, false
	}
	for _, timestamp := range later {
		if !timestamp.After(boundary) {
			return time.Time{}, false
		}
	}
	return boundary, true
}

func parseLTXFilename(name string) (uint64, uint64, bool) {
	base, ok := strings.CutSuffix(name, ".ltx")
	if !ok {
		return 0, 0, false
	}
	minText, maxText, ok := strings.Cut(base, "-")
	if !ok || len(minText) != 16 || len(maxText) != 16 {
		return 0, 0, false
	}
	minTXID, minErr := strconv.ParseUint(minText, 16, 64)
	maxTXID, maxErr := strconv.ParseUint(maxText, 16, 64)
	return minTXID, maxTXID, minErr == nil && maxErr == nil
}

func readLTXHeaderTimestamp(path string, minTXID, maxTXID uint64) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, ltxHeaderTimestampEnd)
	if _, err := io.ReadFull(f, header); err != nil {
		return time.Time{}, err
	}
	if string(header[:4]) != "LTX1" || binary.BigEndian.Uint64(header[16:]) != minTXID || binary.BigEndian.Uint64(header[24:]) != maxTXID {
		return time.Time{}, fmt.Errorf("ltx header does not match %s", filepath.Base(path))
	}
	return time.UnixMilli(int64(binary.BigEndian.Uint64(header[32:]))).UTC(), nil
}

func checkLatestRestoreTXID(restored, boundary uint64) error {
	if restored == 0 {
		return &restoreEvidenceInconclusiveError{err: fmt.Errorf("latest restore txid unavailable: no restore plan evidence in litestream output: boundary=%016x", boundary)}
	}
	if restored < boundary {
		return fmt.Errorf("restore failed: latest restore behind boundary: restored=%016x boundary=%016x", restored, boundary)
	}
	return nil
}
