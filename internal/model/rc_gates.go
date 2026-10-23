package model

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const rcGatesSchema = `
CREATE TABLE IF NOT EXISTS rc_gates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository TEXT NOT NULL,
    candidate_ref TEXT NOT NULL DEFAULT '',
    candidate_source TEXT NOT NULL,
    candidate_sha TEXT NOT NULL,
    candidate_image_ref TEXT NOT NULL DEFAULT '',
    baseline_source TEXT NOT NULL,
    baseline_sha TEXT NOT NULL,
    baseline_image_ref TEXT NOT NULL DEFAULT '',
    duration_seconds INTEGER NOT NULL,
    tail BOOLEAN NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    tail_expires_at DATETIME,
    completed_at DATETIME,
    resources_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_rc_gates_status ON rc_gates(status);
`

type RCGateStatus string

const (
	RCGateRunning   RCGateStatus = "running"
	RCGateTail      RCGateStatus = "tail"
	RCGateCompleted RCGateStatus = "completed"
)

var ErrRCGateSourceActive = errors.New("rc gate source already belongs to an active gate")

type RCGateResource struct {
	Source     string    `json:"source"`
	WorkerName string    `json:"worker_name"`
	Profile    string    `json:"profile"`
	Region     string    `json:"region"`
	CPUs       int       `json:"cpus"`
	MemoryMB   int       `json:"memory_mb"`
	VolumeGB   int       `json:"volume_gb"`
	Tail       bool      `json:"tail"`
	StartedAt  time.Time `json:"started_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type RCGate struct {
	ID                int64            `json:"id"`
	Repository        string           `json:"repository"`
	CandidateRef      string           `json:"candidate_ref,omitempty"`
	CandidateSource   string           `json:"candidate_source"`
	CandidateSHA      string           `json:"candidate_sha"`
	CandidateImageRef string           `json:"candidate_image_ref,omitempty"`
	BaselineSource    string           `json:"baseline_source"`
	BaselineSHA       string           `json:"baseline_sha"`
	BaselineImageRef  string           `json:"baseline_image_ref,omitempty"`
	Duration          time.Duration    `json:"-"`
	Tail              bool             `json:"tail"`
	Status            RCGateStatus     `json:"status"`
	StartedAt         time.Time        `json:"started_at"`
	ExpiresAt         time.Time        `json:"expires_at"`
	TailExpiresAt     *time.Time       `json:"tail_expires_at,omitempty"`
	CompletedAt       *time.Time       `json:"completed_at,omitempty"`
	Resources         []RCGateResource `json:"resources"`
}

func (g RCGate) Sources() []string {
	return []string{g.CandidateSource, g.BaselineSource}
}

func (g RCGate) LitestreamSHAFor(source string) (string, bool) {
	switch source {
	case g.CandidateSource:
		return g.CandidateSHA, true
	case g.BaselineSource:
		return g.BaselineSHA, true
	default:
		return "", false
	}
}

const rcGateColumns = `id, repository, candidate_ref, candidate_source, candidate_sha, candidate_image_ref,
	baseline_source, baseline_sha, baseline_image_ref, duration_seconds, tail, status,
	started_at, expires_at, tail_expires_at, completed_at, resources_json`

func (d *DB) CreateRCGate(gate *RCGate) error {
	if gate == nil {
		return errors.New("rc gate is nil")
	}
	resources, err := json.Marshal(gate.Resources)
	if err != nil {
		return fmt.Errorf("marshal rc gate resources: %w", err)
	}
	tx, err := d.writer.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var overlapping int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM rc_gates
		WHERE status != ? AND (candidate_source IN (?, ?) OR baseline_source IN (?, ?))`,
		RCGateCompleted, gate.CandidateSource, gate.BaselineSource, gate.CandidateSource, gate.BaselineSource,
	).Scan(&overlapping); err != nil {
		return err
	}
	if overlapping > 0 {
		return ErrRCGateSourceActive
	}

	result, err := tx.Exec(`
		INSERT INTO rc_gates (repository, candidate_ref, candidate_source, candidate_sha, candidate_image_ref,
			baseline_source, baseline_sha, baseline_image_ref, duration_seconds, tail, status,
			started_at, expires_at, tail_expires_at, completed_at, resources_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		gate.Repository, gate.CandidateRef, gate.CandidateSource, gate.CandidateSHA, gate.CandidateImageRef,
		gate.BaselineSource, gate.BaselineSHA, gate.BaselineImageRef, int64(gate.Duration/time.Second), gate.Tail, gate.Status,
		gate.StartedAt.UTC(), gate.ExpiresAt.UTC(), utcTimePtr(gate.TailExpiresAt), utcTimePtr(gate.CompletedAt), string(resources),
	)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	gate.ID = id
	return nil
}

func (d *DB) ListRCGates() ([]RCGate, error) {
	return d.listRCGates("SELECT " + rcGateColumns + " FROM rc_gates ORDER BY started_at DESC, id DESC")
}

func (d *DB) ActiveRCGates() ([]RCGate, error) {
	return d.listRCGates("SELECT "+rcGateColumns+" FROM rc_gates WHERE status != ? ORDER BY started_at, id", RCGateCompleted)
}

func (d *DB) ActiveRCGateForSource(source string) (*RCGate, error) {
	source = strings.TrimSpace(source)
	gates, err := d.listRCGates(
		"SELECT "+rcGateColumns+" FROM rc_gates WHERE status != ? AND (candidate_source = ? OR baseline_source = ?) ORDER BY id DESC LIMIT 1",
		RCGateCompleted, source, source,
	)
	if err != nil || len(gates) == 0 {
		return nil, err
	}
	return &gates[0], nil
}

func (d *DB) PinRCGateImage(id int64, source, imageRef string) (string, error) {
	var column string
	var candidate, baseline string
	if err := d.queryRow("SELECT candidate_source, baseline_source FROM rc_gates WHERE id = ?", id).Scan(&candidate, &baseline); err != nil {
		return "", err
	}
	switch source {
	case candidate:
		column = "candidate_image_ref"
	case baseline:
		column = "baseline_image_ref"
	default:
		return "", fmt.Errorf("source %s is not part of rc gate %d", source, id)
	}
	if _, err := d.exec("UPDATE rc_gates SET "+column+" = ? WHERE id = ? AND "+column+" = ''", imageRef, id); err != nil {
		return "", err
	}
	var pinned string
	if err := d.queryRow("SELECT "+column+" FROM rc_gates WHERE id = ?", id).Scan(&pinned); err != nil {
		return "", err
	}
	return pinned, nil
}

func (d *DB) UpdateRCGateStatus(id int64, status RCGateStatus, completedAt *time.Time) error {
	_, err := d.exec("UPDATE rc_gates SET status = ?, completed_at = ? WHERE id = ?", status, utcTimePtr(completedAt), id)
	return err
}

func (d *DB) listRCGates(query string, args ...any) ([]RCGate, error) {
	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	gates := make([]RCGate, 0)
	for rows.Next() {
		var (
			gate            RCGate
			durationSeconds int64
			tailExpiresAt   sql.NullTime
			completedAt     sql.NullTime
			resources       string
		)
		if err := rows.Scan(
			&gate.ID, &gate.Repository, &gate.CandidateRef, &gate.CandidateSource, &gate.CandidateSHA, &gate.CandidateImageRef,
			&gate.BaselineSource, &gate.BaselineSHA, &gate.BaselineImageRef, &durationSeconds, &gate.Tail, &gate.Status,
			&gate.StartedAt, &gate.ExpiresAt, &tailExpiresAt, &completedAt, &resources,
		); err != nil {
			return nil, err
		}
		gate.Duration = time.Duration(durationSeconds) * time.Second
		gate.StartedAt = gate.StartedAt.UTC()
		gate.ExpiresAt = gate.ExpiresAt.UTC()
		if tailExpiresAt.Valid {
			gate.TailExpiresAt = new(tailExpiresAt.Time.UTC())
		}
		if completedAt.Valid {
			gate.CompletedAt = new(completedAt.Time.UTC())
		}
		if err := json.Unmarshal([]byte(resources), &gate.Resources); err != nil {
			return nil, fmt.Errorf("decode rc gate %d resources: %w", gate.ID, err)
		}
		gates = append(gates, gate)
	}
	return gates, rows.Err()
}
