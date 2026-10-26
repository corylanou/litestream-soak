package worker

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/corylanou/litestream-soak/internal/workload"
)

const replicaPasswordEnv = "REPLICA_PASSWORD"

var natsBucketUnsafe = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

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

func applyReplicaBackend(c *Config, getenv func(string) string) error {
	c.ReplicaBackend = workload.NormalizeReplicaBackend(c.ReplicaBackend)
	if c.ReplicaBackend == workload.ReplicaBackendTigris {
		return nil
	}
	c.ArtifactS3 = c.replicaS3Store()
	c.ReplicaBaseURL = strings.TrimRight(strings.TrimSpace(getenv("REPLICA_URL")), "/")
	c.ReplicaPasswordSet = getenv(replicaPasswordEnv) != ""
	c.ReplicaKeyPath = strings.TrimSpace(getenv("REPLICA_KEY_PATH"))
	c.S3FaultProxyEnabled = false
	switch c.ReplicaBackend {
	case workload.ReplicaBackendFile:
		c.ReplicaType = "file"
		if getenv("REPLICA_PATH") == "" {
			c.ReplicaPath = filepath.Join(c.DataDir, "replicas")
		}
		return nil
	case workload.ReplicaBackendMinIO:
		c.ReplicaType = "s3"
		c.S3Endpoint = c.ReplicaBaseURL
		c.S3Bucket = firstNonEmpty(strings.TrimSpace(getenv("REPLICA_BUCKET")), "litestream-soak")
		c.S3AccessKey = getenv("REPLICA_ACCESS_KEY_ID")
		c.S3SecretKey = getenv("REPLICA_SECRET_ACCESS_KEY")
		c.S3SessionToken = ""
		c.S3Region = firstNonEmpty(strings.TrimSpace(getenv("REPLICA_REGION")), "us-east-1")
	case workload.ReplicaBackendSFTP, workload.ReplicaBackendNATS, workload.ReplicaBackendWebDAV:
		c.ReplicaType = c.ReplicaBackend
	default:
		return fmt.Errorf("unsupported REPLICA_BACKEND %q", c.ReplicaBackend)
	}
	if c.ReplicaBaseURL == "" {
		return fmt.Errorf("REPLICA_URL is required when REPLICA_BACKEND=%s", c.ReplicaBackend)
	}
	parsed, err := url.Parse(c.ReplicaBaseURL)
	if err != nil {
		return fmt.Errorf("invalid REPLICA_URL for REPLICA_BACKEND=%s", c.ReplicaBackend)
	}
	if _, hasPassword := parsed.User.Password(); hasPassword {
		return fmt.Errorf("REPLICA_URL must not embed a password; set REPLICA_PASSWORD instead")
	}
	return nil
}

func (c Config) remoteReplicaURL(name string) string {
	prefix := strings.Trim(c.S3Path, "/")
	if c.ReplicaType == workload.ReplicaBackendNATS {
		bucket := prefix
		if name != "" {
			bucket += "-" + name
		}
		return c.ReplicaBaseURL + "/" + natsBucketName(bucket)
	}
	return c.ReplicaBaseURL + "/" + path.Join(prefix, name)
}

func natsBucketName(name string) string {
	return strings.Trim(natsBucketUnsafe.ReplaceAllString(name, "-"), "_-")
}

func (c Config) remoteReplicaType() bool {
	switch c.ReplicaType {
	case workload.ReplicaBackendSFTP, workload.ReplicaBackendNATS, workload.ReplicaBackendWebDAV:
		return true
	default:
		return false
	}
}

func (c Config) NATSBuckets() []string {
	if c.ReplicaType != workload.ReplicaBackendNATS {
		return nil
	}
	var urls []string
	if c.ManyDBEnabled() && c.manyDBConfigMode() == "list" {
		for _, dbPath := range c.ManyDBPaths() {
			urls = append(urls, c.ReplicaURLForDB(dbPath))
		}
	} else {
		urls = append(urls, c.ReplicaURL())
	}
	buckets := make([]string, 0, len(urls))
	for _, replicaURL := range urls {
		buckets = append(buckets, replicaURL[strings.LastIndex(replicaURL, "/")+1:])
	}
	return buckets
}

func (c Config) remoteReplicaExtra() []string {
	var extra []string
	if c.ReplicaPasswordSet {
		key := "password"
		if c.ReplicaType == workload.ReplicaBackendWebDAV {
			key = "webdav-password"
		}
		extra = append(extra, key+": ${"+replicaPasswordEnv+"}")
	}
	if c.ReplicaType == workload.ReplicaBackendSFTP && c.ReplicaKeyPath != "" {
		extra = append(extra, "key-path: "+c.ReplicaKeyPath)
	}
	return extra
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
