package workload

import "strings"

const (
	ReplicaBackendTigris = "tigris"
	ReplicaBackendMinIO  = "minio"
	ReplicaBackendFile   = "file"
	ReplicaBackendSFTP   = "sftp"
	ReplicaBackendNATS   = "nats"
	ReplicaBackendWebDAV = "webdav"
)

func ReplicaBackends() []string {
	return []string{ReplicaBackendTigris, ReplicaBackendMinIO, ReplicaBackendFile, ReplicaBackendSFTP, ReplicaBackendNATS, ReplicaBackendWebDAV}
}

func NormalizeReplicaBackend(backend string) string {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		return ReplicaBackendTigris
	}
	return backend
}

func ReplicaTypeForBackend(backend string) string {
	switch backend = NormalizeReplicaBackend(backend); backend {
	case ReplicaBackendFile, ReplicaBackendSFTP, ReplicaBackendNATS, ReplicaBackendWebDAV:
		return backend
	default:
		return "s3"
	}
}

func (c Config) ReplicaBackendName() string {
	return NormalizeReplicaBackend(c.ReplicaBackend)
}

func (c Config) UsesTigrisReplica() bool {
	return c.ReplicaBackendName() == ReplicaBackendTigris
}
