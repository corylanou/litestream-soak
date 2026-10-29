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
    candidate_git_sha TEXT NOT NULL DEFAULT '',
    baseline_source TEXT NOT NULL,
    baseline_sha TEXT NOT NULL,
    baseline_image_ref TEXT NOT NULL DEFAULT '',
    baseline_git_sha TEXT NOT NULL DEFAULT '',
    duration_seconds INTEGER NOT NULL,
    tail BOOLEAN NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    tail_expires_at DATETIME,
    provision_deadline DATETIME NOT NULL,
    candidate_ready_at DATETIME,
    baseline_ready_at DATETIME,
    paired_ready_at DATETIME,
    completed_at DATETIME,
    resources_json TEXT NOT NULL DEFAULT '[]',
    results_json TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL DEFAULT 'rc',
    upgrade_warmup_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rc_gates_status ON rc_gates(status);
`

type RCGateStatus string

// RCGateKind selects the gate fleet and the scenarios the ledger scores.
type RCGateKind string

const (
	RCGateKindRC      RCGateKind = "rc"
	RCGateKindUpgrade RCGateKind = "upgrade"
)

const (
	RCGateRunning   RCGateStatus = "running"
	RCGateTail      RCGateStatus = "tail"
	RCGateCompleted RCGateStatus = "completed"
	RCGateAborted   RCGateStatus = "aborted"
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
	Kind              RCGateKind       `json:"kind"`
	Repository        string           `json:"repository"`
	CandidateRef      string           `json:"candidate_ref,omitempty"`
	CandidateSource   string           `json:"candidate_source"`
	CandidateSHA      string           `json:"candidate_sha"`
	CandidateImageRef string           `json:"candidate_image_ref,omitempty"`
	CandidateGitSHA   string           `json:"candidate_soak_sha,omitempty"`
	BaselineSource    string           `json:"baseline_source"`
	BaselineSHA       string           `json:"baseline_sha"`
	BaselineImageRef  string           `json:"baseline_image_ref,omitempty"`
	BaselineGitSHA    string           `json:"baseline_soak_sha,omitempty"`
	Duration          time.Duration    `json:"-"`
	UpgradeWarmup     time.Duration    `json:"-"`
	Tail              bool             `json:"tail"`
	Status            RCGateStatus     `json:"status"`
	StartedAt         time.Time        `json:"started_at"`
	ExpiresAt         time.Time        `json:"expires_at"`
	TailExpiresAt     *time.Time       `json:"tail_expires_at,omitempty"`
	ProvisionDeadline time.Time        `json:"provision_deadline"`
	CandidateReadyAt  *time.Time       `json:"candidate_ready_at,omitempty"`
	BaselineReadyAt   *time.Time       `json:"baseline_ready_at,omitempty"`
	PairedReadyAt     *time.Time       `json:"paired_ready_at,omitempty"`
	CompletedAt       *time.Time       `json:"completed_at,omitempty"`
	Resources         []RCGateResource `json:"resources"`
	Results           string           `json:"-"`
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

func (g RCGate) PinnedBuildFor(source string) (imageRef, gitSHA string) {
	switch source {
	case g.CandidateSource:
		return g.CandidateImageRef, g.CandidateGitSHA
	case g.BaselineSource:
		return g.BaselineImageRef, g.BaselineGitSHA
	default:
		return "", ""
	}
}

func (g RCGate) ReadyAtFor(source string) *time.Time {
	switch source {
	case g.CandidateSource:
		return g.CandidateReadyAt
	case g.BaselineSource:
		return g.BaselineReadyAt
	default:
		return nil
	}
}

func (g RCGate) Closed() bool {
	return g.Status == RCGateCompleted || g.Status == RCGateAborted
}

const rcGateColumns = `id, repository, candidate_ref, candidate_source, candidate_sha, candidate_image_ref, candidate_git_sha,
	baseline_source, baseline_sha, baseline_image_ref, baseline_git_sha, duration_seconds, tail, status,
	started_at, expires_at, tail_expires_at, provision_deadline, candidate_ready_at, baseline_ready_at, paired_ready_at, completed_at, resources_json, results_json,
	kind, upgrade_warmup_seconds`

func ensureRCGateColumns(db *sql.DB) error {
	statements := []string{
		`ALTER TABLE rc_gates ADD COLUMN kind TEXT NOT NULL DEFAULT 'rc'`,
		`ALTER TABLE rc_gates ADD COLUMN upgrade_warmup_seconds INTEGER NOT NULL DEFAULT 0`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}
	return nil
}

const rcGateOpenClause = "status NOT IN ('completed', 'aborted')"

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
		WHERE `+rcGateOpenClause+` AND (candidate_source IN (?, ?) OR baseline_source IN (?, ?))`,
		gate.CandidateSource, gate.BaselineSource, gate.CandidateSource, gate.BaselineSource,
	).Scan(&overlapping); err != nil {
		return err
	}
	if overlapping > 0 {
		return ErrRCGateSourceActive
	}

	if gate.Kind == "" {
		gate.Kind = RCGateKindRC
	}
	result, err := tx.Exec(`
		INSERT INTO rc_gates (repository, candidate_ref, candidate_source, candidate_sha, candidate_image_ref, candidate_git_sha,
			baseline_source, baseline_sha, baseline_image_ref, baseline_git_sha, duration_seconds, tail, status,
			started_at, expires_at, tail_expires_at, provision_deadline, candidate_ready_at, baseline_ready_at, paired_ready_at, completed_at, resources_json, results_json,
			kind, upgrade_warmup_seconds)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		gate.Repository, gate.CandidateRef, gate.CandidateSource, gate.CandidateSHA, gate.CandidateImageRef, gate.CandidateGitSHA,
		gate.BaselineSource, gate.BaselineSHA, gate.BaselineImageRef, gate.BaselineGitSHA, int64(gate.Duration/time.Second), gate.Tail, gate.Status,
		gate.StartedAt.UTC(), gate.ExpiresAt.UTC(), utcTimePtr(gate.TailExpiresAt), gate.ProvisionDeadline.UTC(),
		utcTimePtr(gate.CandidateReadyAt), utcTimePtr(gate.BaselineReadyAt), utcTimePtr(gate.PairedReadyAt),
		utcTimePtr(gate.CompletedAt), string(resources), gate.Results,
		string(gate.Kind), int64(gate.UpgradeWarmup/time.Second),
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

func (d *DB) GetRCGate(id int64) (*RCGate, error) {
	gates, err := d.listRCGates("SELECT "+rcGateColumns+" FROM rc_gates WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	if len(gates) == 0 {
		return nil, sql.ErrNoRows
	}
	return &gates[0], nil
}

func (d *DB) ActiveRCGates() ([]RCGate, error) {
	return d.listRCGates("SELECT " + rcGateColumns + " FROM rc_gates WHERE " + rcGateOpenClause + " ORDER BY started_at, id")
}

func (d *DB) ActiveRCGateForSource(source string) (*RCGate, error) {
	source = strings.TrimSpace(source)
	gates, err := d.listRCGates(
		"SELECT "+rcGateColumns+" FROM rc_gates WHERE "+rcGateOpenClause+" AND (candidate_source = ? OR baseline_source = ?) ORDER BY id DESC LIMIT 1",
		source, source,
	)
	if err != nil || len(gates) == 0 {
		return nil, err
	}
	return &gates[0], nil
}

func (d *DB) PinRCGateBuild(id int64, source, imageRef, gitSHA string) (string, string, error) {
	var candidate, baseline string
	if err := d.queryRow("SELECT candidate_source, baseline_source FROM rc_gates WHERE id = ?", id).Scan(&candidate, &baseline); err != nil {
		return "", "", err
	}
	var prefix string
	switch source {
	case candidate:
		prefix = "candidate"
	case baseline:
		prefix = "baseline"
	default:
		return "", "", fmt.Errorf("source %s is not part of rc gate %d", source, id)
	}
	imageColumn, gitColumn := prefix+"_image_ref", prefix+"_git_sha"
	if _, err := d.exec(
		"UPDATE rc_gates SET "+imageColumn+" = ?, "+gitColumn+" = ? WHERE id = ? AND "+imageColumn+" = '' AND "+rcGateOpenClause,
		imageRef, gitSHA, id,
	); err != nil {
		return "", "", err
	}
	var pinnedImage, pinnedGit string
	if err := d.queryRow("SELECT "+imageColumn+", "+gitColumn+" FROM rc_gates WHERE id = ?", id).Scan(&pinnedImage, &pinnedGit); err != nil {
		return "", "", err
	}
	return pinnedImage, pinnedGit, nil
}

func (d *DB) MarkRCGateSideReady(id int64, source string, at time.Time) error {
	var candidate, baseline string
	if err := d.queryRow("SELECT candidate_source, baseline_source FROM rc_gates WHERE id = ?", id).Scan(&candidate, &baseline); err != nil {
		return err
	}
	var column string
	switch source {
	case candidate:
		column = "candidate_ready_at"
	case baseline:
		column = "baseline_ready_at"
	default:
		return fmt.Errorf("source %s is not part of rc gate %d", source, id)
	}
	_, err := d.exec("UPDATE rc_gates SET "+column+" = ? WHERE id = ? AND "+column+" IS NULL", at.UTC(), id)
	return err
}

func (d *DB) MarkRCGatePairedReady(id int64, at time.Time) error {
	_, err := d.exec("UPDATE rc_gates SET paired_ready_at = ? WHERE id = ? AND paired_ready_at IS NULL", at.UTC(), id)
	return err
}

func (d *DB) UpdateRCGateStatus(id int64, status RCGateStatus, completedAt *time.Time, results string) error {
	_, err := d.exec(
		"UPDATE rc_gates SET status = ?, completed_at = ?, results_json = CASE WHEN ? = '' THEN results_json ELSE ? END WHERE id = ?",
		status, utcTimePtr(completedAt), results, results, id,
	)
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
			warmupSeconds   int64
			kind            string
			tailExpiresAt   sql.NullTime
			candidateReady  sql.NullTime
			baselineReady   sql.NullTime
			pairedReadyAt   sql.NullTime
			completedAt     sql.NullTime
			resources       string
		)
		if err := rows.Scan(
			&gate.ID, &gate.Repository, &gate.CandidateRef, &gate.CandidateSource, &gate.CandidateSHA, &gate.CandidateImageRef, &gate.CandidateGitSHA,
			&gate.BaselineSource, &gate.BaselineSHA, &gate.BaselineImageRef, &gate.BaselineGitSHA, &durationSeconds, &gate.Tail, &gate.Status,
			&gate.StartedAt, &gate.ExpiresAt, &tailExpiresAt, &gate.ProvisionDeadline, &candidateReady, &baselineReady, &pairedReadyAt, &completedAt, &resources, &gate.Results,
			&kind, &warmupSeconds,
		); err != nil {
			return nil, err
		}
		gate.Duration = time.Duration(durationSeconds) * time.Second
		gate.UpgradeWarmup = time.Duration(warmupSeconds) * time.Second
		gate.Kind = RCGateKind(kind)
		if gate.Kind == "" {
			gate.Kind = RCGateKindRC
		}
		gate.StartedAt = gate.StartedAt.UTC()
		gate.ExpiresAt = gate.ExpiresAt.UTC()
		gate.ProvisionDeadline = gate.ProvisionDeadline.UTC()
		for _, value := range []struct {
			source sql.NullTime
			target **time.Time
		}{
			{tailExpiresAt, &gate.TailExpiresAt},
			{candidateReady, &gate.CandidateReadyAt},
			{baselineReady, &gate.BaselineReadyAt},
			{pairedReadyAt, &gate.PairedReadyAt},
			{completedAt, &gate.CompletedAt},
		} {
			if value.source.Valid {
				*value.target = new(value.source.Time.UTC())
			}
		}
		if err := json.Unmarshal([]byte(resources), &gate.Resources); err != nil {
			return nil, fmt.Errorf("decode rc gate %d resources: %w", gate.ID, err)
		}
		gates = append(gates, gate)
	}
	return gates, rows.Err()
}
