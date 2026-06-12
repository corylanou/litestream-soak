package replay

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	gharchiveDroppedPayloadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "soak_replay_gharchive_dropped_payloads_total",
		Help: "Total gharchive event payloads dropped due to JSON parse failures.",
	}, []string{"event_type"})
)

type GHArchiveAdapter struct {
	dataPath string
}

func NewGHArchiveAdapter(dataPath string) *GHArchiveAdapter {
	return &GHArchiveAdapter{dataPath: dataPath}
}

func (a *GHArchiveAdapter) Name() string { return "gharchive" }

func (a *GHArchiveAdapter) CreateTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS gh_events (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			actor_login TEXT,
			repo_name TEXT,
			created_at TEXT NOT NULL,
			payload TEXT,
			received_at TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE INDEX IF NOT EXISTS idx_gh_events_type ON gh_events(type);
		CREATE INDEX IF NOT EXISTS idx_gh_events_created ON gh_events(created_at);

		CREATE TABLE IF NOT EXISTS gh_push_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL,
			repo_name TEXT,
			actor_login TEXT,
			ref TEXT,
			commit_count INTEGER,
			created_at TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS gh_issue_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL,
			repo_name TEXT,
			actor_login TEXT,
			action TEXT,
			issue_number INTEGER,
			title TEXT,
			created_at TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS gh_pr_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL,
			repo_name TEXT,
			actor_login TEXT,
			action TEXT,
			pr_number INTEGER,
			title TEXT,
			created_at TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS gh_watch_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL,
			repo_name TEXT,
			actor_login TEXT,
			created_at TEXT NOT NULL
		);
	`)
	return err
}

func (a *GHArchiveAdapter) Rows() (RowIterator, error) {
	f, err := os.Open(a.dataPath)
	if err != nil {
		return nil, fmt.Errorf("open gharchive data: %w", err)
	}

	var reader io.ReadCloser
	if len(a.dataPath) > 3 && a.dataPath[len(a.dataPath)-3:] == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		reader = gz
	} else {
		reader = f
	}

	return &ghArchiveIterator{
		file:    f,
		reader:  reader,
		decoder: json.NewDecoder(reader),
	}, nil
}

type ghEvent struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Actor struct {
		Login string `json:"login"`
	} `json:"actor"`
	Repo struct {
		Name string `json:"name"`
	} `json:"repo"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type ghArchiveIterator struct {
	file    *os.File
	reader  io.ReadCloser
	decoder *json.Decoder
	event   ghEvent
	err     error
}

func (it *ghArchiveIterator) Next() bool {
	it.err = it.decoder.Decode(&it.event)
	if it.err == io.EOF {
		it.err = nil
		return false
	}
	return it.err == nil
}

func (it *ghArchiveIterator) Timestamp() time.Time {
	return it.event.CreatedAt
}

func (it *ghArchiveIterator) Insert(db *sql.DB) error {
	e := it.event
	ts := e.CreatedAt.Format(time.RFC3339)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec(`INSERT OR IGNORE INTO gh_events (id, type, actor_login, repo_name, created_at, payload) VALUES (?, ?, ?, ?, ?, ?)`,
		e.ID, e.Type, e.Actor.Login, e.Repo.Name, ts, string(e.Payload))
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if affected == 0 {
		return tx.Commit()
	}

	switch e.Type {
	case "PushEvent":
		var p struct {
			Ref     string `json:"ref"`
			Commits []any  `json:"commits"`
		}
		if uerr := json.Unmarshal(e.Payload, &p); uerr != nil {
			slog.Warn("gharchive payload parse failed", "event_id", e.ID, "type", e.Type, "error", uerr)
			gharchiveDroppedPayloadsTotal.WithLabelValues(e.Type).Inc()
			break
		}
		_, err = tx.Exec(`INSERT INTO gh_push_events (event_id, repo_name, actor_login, ref, commit_count, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			e.ID, e.Repo.Name, e.Actor.Login, p.Ref, len(p.Commits), ts)

	case "IssuesEvent":
		var p struct {
			Action string `json:"action"`
			Issue  struct {
				Number int    `json:"number"`
				Title  string `json:"title"`
			} `json:"issue"`
		}
		if uerr := json.Unmarshal(e.Payload, &p); uerr != nil {
			slog.Warn("gharchive payload parse failed", "event_id", e.ID, "type", e.Type, "error", uerr)
			gharchiveDroppedPayloadsTotal.WithLabelValues(e.Type).Inc()
			break
		}
		_, err = tx.Exec(`INSERT INTO gh_issue_events (event_id, repo_name, actor_login, action, issue_number, title, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			e.ID, e.Repo.Name, e.Actor.Login, p.Action, p.Issue.Number, p.Issue.Title, ts)

	case "PullRequestEvent":
		var p struct {
			Action      string `json:"action"`
			PullRequest struct {
				Number int    `json:"number"`
				Title  string `json:"title"`
			} `json:"pull_request"`
		}
		if uerr := json.Unmarshal(e.Payload, &p); uerr != nil {
			slog.Warn("gharchive payload parse failed", "event_id", e.ID, "type", e.Type, "error", uerr)
			gharchiveDroppedPayloadsTotal.WithLabelValues(e.Type).Inc()
			break
		}
		_, err = tx.Exec(`INSERT INTO gh_pr_events (event_id, repo_name, actor_login, action, pr_number, title, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			e.ID, e.Repo.Name, e.Actor.Login, p.Action, p.PullRequest.Number, p.PullRequest.Title, ts)

	case "WatchEvent":
		_, err = tx.Exec(`INSERT INTO gh_watch_events (event_id, repo_name, actor_login, created_at) VALUES (?, ?, ?, ?)`,
			e.ID, e.Repo.Name, e.Actor.Login, ts)
	}

	if err != nil {
		return err
	}

	return tx.Commit()
}

func (it *ghArchiveIterator) Err() error { return it.err }

func (it *ghArchiveIterator) Close() error {
	if it.reader == it.file {
		return it.file.Close()
	}
	return errors.Join(it.reader.Close(), it.file.Close())
}
