package flyapi

import "time"

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
	CreatedAt         time.Time `json:"created_at"`
}

type CreateMachineRequest struct {
	Name   string        `json:"name,omitempty"`
	Region string        `json:"region,omitempty"`
	Config MachineConfig `json:"config"`
}

type CreateVolumeRequest struct {
	Name      string `json:"name"`
	SizeGB    int    `json:"size_gb"`
	Region    string `json:"region"`
	SourceID  string `json:"source_vol_id,omitempty"`
	Encrypted bool   `json:"encrypted"`
}
