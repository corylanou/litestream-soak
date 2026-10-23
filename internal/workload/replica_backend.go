package workload

import "strings"

const (
	ReplicaBackendTigris       = "tigris"
	ReplicaBackendMinIOSidecar = "minio-sidecar"
	ReplicaBackendFile         = "file"
)

func NormalizeReplicaBackend(backend string) string {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		return ReplicaBackendTigris
	}
	return backend
}

func ReplicaTypeForBackend(backend string) string {
	if NormalizeReplicaBackend(backend) == ReplicaBackendFile {
		return "file"
	}
	return "s3"
}

func (c Config) ReplicaBackendName() string {
	return NormalizeReplicaBackend(c.ReplicaBackend)
}

func (c Config) UsesTigrisReplica() bool {
	return c.ReplicaBackendName() == ReplicaBackendTigris
}
