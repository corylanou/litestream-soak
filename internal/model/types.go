package model

import "time"

type WorkerStatus string

const (
	WorkerPending  WorkerStatus = "pending"
	WorkerBuilding WorkerStatus = "building"
	WorkerStarting WorkerStatus = "starting"
	WorkerRunning  WorkerStatus = "running"
	WorkerDegraded WorkerStatus = "degraded"
	WorkerStopped  WorkerStatus = "stopped"
	WorkerFailed   WorkerStatus = "failed"
)

type Worker struct {
	ID              string       `json:"id"`
	AppName         string       `json:"app_name,omitempty"`
	Region          string       `json:"region,omitempty"`
	FlyMachineID    string       `json:"fly_machine_id"`
	FlyVolumeID     string       `json:"fly_volume_id"`
	Name            string       `json:"name"`
	Status          WorkerStatus `json:"status"`
	Source          string       `json:"source"`
	GitSHA          string       `json:"git_sha"`
	PRNumber        int          `json:"pr_number,omitempty"`
	ProfileName     string       `json:"profile_name"`
	ProfileConfig   string       `json:"profile_config"`
	ExpiresAt       *time.Time   `json:"expires_at,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
	LastHeartbeatAt *time.Time   `json:"last_heartbeat_at,omitempty"`
	ErrorMessage    string       `json:"error_message,omitempty"`
	LastRuntimeJSON string       `json:"-"`
	LastRuntimeAt   *time.Time   `json:"-"`
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

type Deployment struct {
	ID           int        `json:"id"`
	GitSHA       string     `json:"git_sha"`
	ImageRef     string     `json:"image_ref"`
	Source       string     `json:"source"`
	PRNumber     int        `json:"pr_number,omitempty"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

type Event struct {
	ID        int       `json:"id"`
	WorkerID  string    `json:"worker_id,omitempty"`
	EventType string    `json:"event_type"`
	Message   string    `json:"message"`
	Details   string    `json:"details,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type AlertDelivery struct {
	ID               int        `json:"id"`
	WorkerID         string     `json:"worker_id,omitempty"`
	VerificationID   int        `json:"verification_id,omitempty"`
	AlertType        string     `json:"alert_type"`
	Fingerprint      string     `json:"fingerprint"`
	Status           string     `json:"status"`
	FailureStage     string     `json:"failure_stage,omitempty"`
	FailureSignature string     `json:"failure_signature,omitempty"`
	Message          string     `json:"message,omitempty"`
	Payload          string     `json:"payload,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	SentAt           *time.Time `json:"sent_at,omitempty"`
}
