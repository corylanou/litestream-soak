package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"modernc.org/sqlite"
)

const heavyLoadMaxRows = 100_000
const loadRetentionBatchSize = 10_000
const loadRetentionBusyTimeout = 5 * time.Second
const loadRetentionRetryDelay = 100 * time.Millisecond
const loadRetentionMaxRetryDelay = time.Second

type loadRetentionStats struct {
	Retries     int
	RowsTrimmed int64
	Completed   bool
}

func (s loadRetentionStats) outputTail() string {
	return fmt.Sprintf("retries=%d rows_trimmed=%d completed=%t", s.Retries, s.RowsTrimmed, s.Completed)
}

func (c Config) boundedLoadProfile() bool {
	switch c.ProfileName {
	case "high-volume", "high-vol-ams", "burst-volume", "gharchive-replay", "gharchive-mixed", "pinned-reader", "overload-truncate0":
		return !c.ManyDBEnabled() && !c.churnEnabled()
	default:
		return false
	}
}

func installLoadRetention(ctx context.Context, cfg Config, maxRows int) (loadRetentionStats, error) {
	stats := loadRetentionStats{}
	if !cfg.boundedLoadProfile() {
		stats.Completed = true
		return stats, nil
	}
	if maxRows <= 0 {
		return stats, fmt.Errorf("load retention requires a positive row cap")
	}
	uri := url.URL{Scheme: "file", Path: cfg.DBPath, RawQuery: fmt.Sprintf("mode=rw&_pragma=busy_timeout(%d)", loadRetentionBusyTimeout.Milliseconds())}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return stats, err
	}
	defer func() { _ = db.Close() }()
	completed := true
	for _, table := range []string{"load_test", "gh_events", "gh_push_events", "gh_issue_events", "gh_pr_events", "gh_watch_events"} {
		var exists int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?", table).Scan(&exists); err != nil {
			return stats, err
		}
		if exists == 0 {
			continue
		}
		trigger := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS soak_retain_%s AFTER INSERT ON %s BEGIN DELETE FROM %s WHERE rowid <= NEW.rowid - %d; END`, table, table, table, maxRows)
		if err := retryLoadRetentionWrite(ctx, &stats, func() error {
			_, err := db.ExecContext(ctx, trigger)
			return err
		}); err != nil {
			return stats, fmt.Errorf("install %s retention: %w", table, err)
		}
		result, err := retryLoadRetentionExec(ctx, &stats, func() (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE rowid <= (SELECT max(rowid) FROM %s) - ? LIMIT %d)`, table, table, table, loadRetentionBatchSize), maxRows)
		})
		if err != nil {
			return stats, fmt.Errorf("trim %s: %w", table, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return stats, err
		}
		stats.RowsTrimmed += n
		if n == loadRetentionBatchSize {
			completed = false
		}
	}
	stats.Completed = completed
	return stats, nil
}

func retryLoadRetentionWrite(ctx context.Context, stats *loadRetentionStats, operation func() error) error {
	_, err := retryLoadRetentionExec(ctx, stats, func() (sql.Result, error) {
		return nil, operation()
	})
	return err
}

func retryLoadRetentionExec(ctx context.Context, stats *loadRetentionStats, operation func() (sql.Result, error)) (sql.Result, error) {
	delay := loadRetentionRetryDelay
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := operation()
		if err == nil {
			return result, nil
		}
		if !isLoadRetentionBusy(err) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stats.Retries++
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, loadRetentionMaxRetryDelay)
	}
}

func isLoadRetentionBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff
	return code == 5 || code == 6
}
