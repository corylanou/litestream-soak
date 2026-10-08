package worker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
)

const heavyLoadMaxRows = 100_000

func (c Config) boundedLoadProfile() bool {
	switch c.ProfileName {
	case "high-volume", "high-vol-ams", "burst-volume", "gharchive-replay", "gharchive-mixed", "pinned-reader", "overload-truncate0":
		return !c.ManyDBEnabled() && !c.churnEnabled()
	default:
		return false
	}
}

func installLoadRetention(ctx context.Context, cfg Config, maxRows int) error {
	if !cfg.boundedLoadProfile() {
		return nil
	}
	if maxRows <= 0 {
		return fmt.Errorf("load retention requires a positive row cap")
	}
	uri := url.URL{Scheme: "file", Path: cfg.DBPath, RawQuery: "mode=rw&_pragma=busy_timeout(5000)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	for _, table := range []string{"load_test", "gh_events", "gh_push_events", "gh_issue_events", "gh_pr_events", "gh_watch_events"} {
		var exists int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?", table).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			continue
		}
		trigger := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS soak_retain_%s AFTER INSERT ON %s BEGIN DELETE FROM %s WHERE rowid <= NEW.rowid - %d; END`, table, table, table, maxRows)
		if _, err := db.ExecContext(ctx, trigger); err != nil {
			return fmt.Errorf("install %s retention: %w", table, err)
		}
		for {
			result, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE rowid <= (SELECT max(rowid) FROM %s) - ? LIMIT 10000)`, table, table, table), maxRows)
			if err != nil {
				return fmt.Errorf("trim %s: %w", table, err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
		}
	}
	return nil
}
