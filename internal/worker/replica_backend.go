package worker

import (
	"fmt"
	"path/filepath"

	"github.com/corylanou/litestream-soak/internal/workload"
)

const minioSidecarBucket = "litestream-soak"

type s3Store struct {
	Bucket       string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Region       string
}

func (c Config) replicaS3Store() s3Store {
	return s3Store{
		Bucket:       c.S3Bucket,
		Endpoint:     c.S3Endpoint,
		AccessKey:    c.S3AccessKey,
		SecretKey:    c.S3SecretKey,
		SessionToken: c.S3SessionToken,
		Region:       c.S3Region,
	}
}

func applyReplicaBackend(c *Config, explicitReplicaPath bool) error {
	c.ReplicaBackend = workload.NormalizeReplicaBackend(c.ReplicaBackend)
	switch c.ReplicaBackend {
	case workload.ReplicaBackendTigris:
		return nil
	case workload.ReplicaBackendFile:
		c.ArtifactS3 = c.replicaS3Store()
		c.ReplicaType = "file"
		if !explicitReplicaPath {
			c.ReplicaPath = filepath.Join(c.DataDir, "replicas")
		}
	case workload.ReplicaBackendMinIOSidecar:
		c.ArtifactS3 = c.replicaS3Store()
		c.ReplicaType = "s3"
		c.S3Bucket = minioSidecarBucket
		c.S3Endpoint = "http://" + c.MinIOAddr
		c.S3AccessKey = ""
		c.S3SecretKey = ""
		c.S3SessionToken = ""
		c.S3Region = "us-east-1"
		c.MinIODataDir = filepath.Join(c.DataDir, "minio")
	default:
		return fmt.Errorf("unsupported REPLICA_BACKEND %q", c.ReplicaBackend)
	}
	c.S3FaultProxyEnabled = false
	return nil
}

func (c Config) artifactStore() (s3Store, bool) {
	if workload.NormalizeReplicaBackend(c.ReplicaBackend) != workload.ReplicaBackendTigris {
		return c.ArtifactS3, c.ArtifactS3.Bucket != ""
	}
	if c.ReplicaType != "s3" {
		return s3Store{}, false
	}
	store := c.replicaS3Store()
	store.Endpoint = c.directS3Endpoint()
	return store, true
}

func (c Config) profileUploadEnabled() bool {
	_, ok := c.artifactStore()
	return ok
}
