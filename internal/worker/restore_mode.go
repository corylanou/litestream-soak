package worker

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	restoreModeTXID                  = "txid"
	restoreModeTimestamp             = "timestamp"
	restoreModeLatest                = "latest"
	restoreModeFallbackNoLTXBoundary = "timestamp_no_ltx_boundary"
	ltxHeaderTimestampEnd            = 40
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

func (v *Verifier) nextRestoreMode() string {
	mode := restoreModeRotation[v.restoreCycles%len(restoreModeRotation)]
	v.restoreCycles++
	return mode
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
		return fmt.Errorf("restore failed: latest restore txid unavailable: boundary=%016x", boundary)
	}
	if restored < boundary {
		return fmt.Errorf("restore failed: latest restore behind boundary: restored=%016x boundary=%016x", restored, boundary)
	}
	return nil
}
