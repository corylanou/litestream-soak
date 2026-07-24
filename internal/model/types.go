package model

import (
	"strings"
	"time"
)

type WorkerStatus string

const (
	WorkerPending  WorkerStatus = "pending"
	WorkerBuilding WorkerStatus = "building"
	WorkerStarting WorkerStatus = "starting"
	WorkerRunning  WorkerStatus = "running"
	WorkerDegraded WorkerStatus = "degraded"
	WorkerDormant  WorkerStatus = "dormant"
	WorkerProbing  WorkerStatus = "probing"
	WorkerStopped  WorkerStatus = "stopped"
	WorkerFailed   WorkerStatus = "failed"
)

type Worker struct {
	ID               string       `json:"id"`
	AppName          string       `json:"app_name,omitempty"`
	Region           string       `json:"region,omitempty"`
	FlyMachineID     string       `json:"fly_machine_id"`
	FlyVolumeID      string       `json:"fly_volume_id"`
	Name             string       `json:"name"`
	Status           WorkerStatus `json:"status"`
	Source           string       `json:"source"`
	GitSHA           string       `json:"git_sha"`
	LitestreamSHA    string       `json:"litestream_sha,omitempty"`
	PRNumber         int          `json:"pr_number,omitempty"`
	ProfileName      string       `json:"profile_name"`
	ProfileConfig    string       `json:"profile_config"`
	ExpiresAt        *time.Time   `json:"expires_at,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
	LastHeartbeatAt  *time.Time   `json:"last_heartbeat_at,omitempty"`
	ErrorMessage     string       `json:"error_message,omitempty"`
	LastRuntimeJSON  string       `json:"-"`
	LastRuntimeAt    *time.Time   `json:"-"`
	DormantAt        *time.Time   `json:"dormant_at,omitempty"`
	DormantReason    string       `json:"dormant_reason,omitempty"`
	DormantSignature string       `json:"dormant_signature,omitempty"`
	ResumeTrigger    string       `json:"resume_trigger,omitempty"`
	LastProbeAt      *time.Time   `json:"last_probe_at,omitempty"`
}

type Verification struct {
	ID               int        `json:"id"`
	WorkerID         string     `json:"worker_id"`
	StartedAt        time.Time  `json:"started_at"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
	Status           string     `json:"status"`
	CheckType        string     `json:"check_type"`
	SourceChecksum   string     `json:"source_checksum,omitempty"`
	RestoredChecksum string     `json:"restored_checksum,omitempty"`
	Passed           bool       `json:"passed"`
	DurationMS       int        `json:"duration_ms,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
}

func (v Verification) Aborted() bool {
	return strings.EqualFold(strings.TrimSpace(v.Status), "aborted")
}

func (v Verification) Failed() bool {
	return !v.Aborted() && (!v.Passed || strings.EqualFold(strings.TrimSpace(v.Status), "failed"))
}

func (v Verification) Succeeded() bool {
	return !v.Aborted() && v.Passed && !strings.EqualFold(strings.TrimSpace(v.Status), "failed")
}

type Deployment struct {
	ID            int        `json:"id"`
	GitSHA        string     `json:"git_sha"`
	LitestreamSHA string     `json:"litestream_sha,omitempty"`
	ImageRef      string     `json:"image_ref"`
	Source        string     `json:"source"`
	PRNumber      int        `json:"pr_number,omitempty"`
	Status        string     `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	ErrorMessage  string     `json:"error_message,omitempty"`
}

type Event struct {
	ID                   int        `json:"id"`
	WorkerID             string     `json:"worker_id,omitempty"`
	EventType            string     `json:"event_type"`
	Message              string     `json:"message"`
	Details              string     `json:"details,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	CollapsedCount       int        `json:"collapsed_count,omitempty"`
	CollapsedWindowStart *time.Time `json:"collapsed_window_start,omitempty"`
	CollapsedWindowEnd   *time.Time `json:"collapsed_window_end,omitempty"`
}

type RunArchive struct {
	ID            int       `json:"id"`
	DeploymentID  int       `json:"deployment_id"`
	Source        string    `json:"source"`
	WorkerID      string    `json:"worker_id,omitempty"`
	ArchiveType   string    `json:"archive_type"`
	GitSHA        string    `json:"git_sha"`
	LitestreamSHA string    `json:"litestream_sha,omitempty"`
	ImageRef      string    `json:"image_ref,omitempty"`
	Status        string    `json:"status"`
	Summary       string    `json:"summary"`
	Payload       string    `json:"payload"`
	ArchivedAt    time.Time `json:"archived_at"`
}

type AlertDelivery struct {
	ID                  int        `json:"id"`
	WorkerID            string     `json:"worker_id,omitempty"`
	VerificationID      int        `json:"verification_id,omitempty"`
	Source              string     `json:"source,omitempty"`
	AlertType           string     `json:"alert_type"`
	Fingerprint         string     `json:"fingerprint"`
	Status              string     `json:"status"`
	ConditionStatus     string     `json:"condition_status,omitempty"`
	ConditionStartedAt  *time.Time `json:"condition_started_at,omitempty"`
	ConditionResolvedAt *time.Time `json:"condition_resolved_at,omitempty"`
	FailureStage        string     `json:"failure_stage,omitempty"`
	FailureSignature    string     `json:"failure_signature,omitempty"`
	Message             string     `json:"message,omitempty"`
	Payload             string     `json:"payload,omitempty"`
	ErrorMessage        string     `json:"error_message,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	SentAt              *time.Time `json:"sent_at,omitempty"`
}
