package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/rig"
	"github.com/corylanou/litestream-soak/internal/worker"
	_ "modernc.org/sqlite"
)

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0"); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

func appendRow(ctx context.Context, db *sql.DB, id int) error {
	_, err := db.ExecContext(ctx, "INSERT INTO t(id,value) VALUES(?,?)", id, fmt.Sprintf("row-%08d-%s", id, strings.Repeat("x", 65536)))
	return err
}

func validate(ctx context.Context, source *sql.DB, path string, minimum int) (int, error) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return 0, err
	}
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return 0, err
	}
	if integrity != "ok" {
		return 0, fmt.Errorf("integrity check: %s", integrity)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&count); err != nil {
		return 0, err
	}
	if err := rig.ValidatePrefix(ctx, source, db, minimum, 1); err != nil {
		return count, err
	}
	var sourcePath string
	if err := source.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&sourcePath); err != nil {
		return count, err
	}
	return count, worker.CompareLogicalSchemas(ctx, sourcePath, path)
}

var messagePattern = regexp.MustCompile(`msg=("[^"]*"|[^ ]+)`)

var deletionPattern = regexp.MustCompile(`deleted_count[=:]([0-9]+)`)

func maintenance(log string) (int, int, []string) {
	compactions, deletions := 0, 0
	var incidents []string
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, `msg="compaction complete"`) {
			compactions++
		}
		if strings.Contains(line, "retention") {
			if m := deletionPattern.FindStringSubmatch(line); len(m) == 2 {
				n, _ := strconv.Atoi(m[1])
				deletions += n
			}
		}
		message := line
		if match := messagePattern.FindStringSubmatch(line); len(match) == 2 {
			message = match[1]
		}
		lower := strings.ToLower(message)
		if strings.Contains(line, "level=ERROR") || strings.Contains(line, "level=WARN") || strings.Contains(lower, "retry") || strings.Contains(lower, "self-heal") {
			incidents = append(incidents, line)
		}
	}
	return compactions, deletions, incidents
}

func restoreExposed(log, restoreLog string, start, end time.Time) bool {
	var during strings.Builder
	for _, line := range strings.Split(log, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		stamp, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(fields[0], "time="))
		if err == nil && !stamp.Before(start) && !stamp.After(end) {
			during.WriteString(line)
			during.WriteByte('\n')
		}
	}
	c, d, _ := maintenance(during.String())
	opened := false
	for _, line := range strings.Split(restoreLog, "\n") {
		opened = opened || (strings.Contains(line, "opening ltx file for restore") && strings.Contains(line, "level=0"))
	}
	return c > 0 && d > 0 && opened
}
