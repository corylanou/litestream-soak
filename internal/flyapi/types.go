package flyapi

import (
	"fmt"
	"time"
)

type Machine struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	State      string         `json:"state"`
	Region     string         `json:"region"`
	InstanceID string         `json:"instance_id"`
	Config     MachineConfig  `json:"config"`
	ImageRef   ImageRef       `json:"image_ref"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	Events     []MachineEvent `json:"events"`
}

type MachineConfig struct {
	Image    string            `json:"image"`
	Env      map[string]string `json:"env,omitempty"`
	Guest    Guest             `json:"guest,omitempty"`
	Mounts   []Mount           `json:"mounts,omitempty"`
	Metrics  *MetricsConfig    `json:"metrics,omitempty"`
	Services []Service         `json:"services,omitempty"`
	// StopConfig sets how long Fly waits after the stop signal before it
	// kills the machine. Without it Fly waits five seconds.
	StopConfig *StopConfig `json:"stop_config,omitempty"`
}

// WorkerStopTimeout is the kill grace every worker machine is created with
// and every stop request asks for. A worker that is told to stop lets
// Litestream exit, sends its final heartbeat and flushes pending evidence;
// a rollout deletes the old volume afterwards, so evidence still on it when
// the machine is killed is lost. Fly allows at most five minutes.
const WorkerStopTimeout = 150 * time.Second

type StopConfig struct {
	Timeout string `json:"timeout,omitempty"`
	Signal  string `json:"signal,omitempty"`
}

// WorkerStopConfig is the stop configuration for worker machines and stop
// requests.
func WorkerStopConfig() *StopConfig {
	return &StopConfig{Timeout: fmt.Sprintf("%ds", int(WorkerStopTimeout/time.Second)), Signal: "SIGTERM"}
}

type Guest struct {
	CPUKind  string `json:"cpu_kind,omitempty"`
	CPUs     int    `json:"cpus,omitempty"`
	MemoryMB int    `json:"memory_mb,omitempty"`
}

type Mount struct {
	Volume string `json:"volume"`
	Path   string `json:"path"`
}

type MetricsConfig struct {
	Port int    `json:"port"`
	Path string `json:"path"`
}

type Service struct {
	InternalPort int           `json:"internal_port"`
	Protocol     string        `json:"protocol"`
	Ports        []ServicePort `json:"ports,omitempty"`
}

type ServicePort struct {
	Port int `json:"port"`
}

type ImageRef struct {
	Registry   string `json:"registry"`
	Repository string `json:"repository"`
	Tag        string `json:"tag"`
	Digest     string `json:"digest"`
}

type MachineEvent struct {
	Type      string `json:"type"`
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

type AppLogsResponse struct {
	Data []AppLogEntry `json:"data"`
	Meta AppLogsMeta   `json:"meta"`
}

type AppLogsMeta struct {
	NextToken string `json:"next_token"`
}

type AppLogEntry struct {
	ID         string           `json:"id"`
	Type       string           `json:"type"`
	Attributes AppLogAttributes `json:"attributes"`
}

type AppLogAttributes struct {
	Timestamp time.Time  `json:"timestamp"`
	Message   string     `json:"message"`
	Level     string     `json:"level"`
	Instance  string     `json:"instance"`
	Region    string     `json:"region"`
	Meta      AppLogMeta `json:"meta"`
}

type AppLogMeta struct {
	Event AppLogMetaEvent `json:"event"`
}

type AppLogMetaEvent struct {
	Provider string `json:"provider"`
}

type Volume struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	State             string    `json:"state"`
	SizeGB            int       `json:"size_gb"`
	Region            string    `json:"region"`
	AttachedMachineID string    `json:"attached_machine_id"`
	AttachedAllocID   string    `json:"attached_alloc_id"`
	CreatedAt         time.Time `json:"created_at"`
}

type CreateMachineRequest struct {
	Name   string        `json:"name,omitempty"`
	Region string        `json:"region,omitempty"`
	Config MachineConfig `json:"config"`
}

type CreateVolumeRequest struct {
	Name      string `json:"name"`
	SizeGB    int    `json:"size_gb,omitempty"`
	Region    string `json:"region,omitempty"`
	SourceID  string `json:"source_volume_id,omitempty"`
	Encrypted bool   `json:"encrypted"`
}
