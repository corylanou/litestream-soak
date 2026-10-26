package workload

import "testing"

func TestReplicaBackendDefaultsToTigris(t *testing.T) {
	t.Parallel()

	tests := []struct {
		backend     string
		want        string
		replicaType string
		usesTigris  bool
	}{
		{backend: "", want: ReplicaBackendTigris, replicaType: "s3", usesTigris: true},
		{backend: " Tigris ", want: ReplicaBackendTigris, replicaType: "s3", usesTigris: true},
		{backend: "minio", want: ReplicaBackendMinIO, replicaType: "s3", usesTigris: false},
		{backend: "file", want: ReplicaBackendFile, replicaType: "file", usesTigris: false},
		{backend: "sftp", want: ReplicaBackendSFTP, replicaType: "sftp", usesTigris: false},
		{backend: "nats", want: ReplicaBackendNATS, replicaType: "nats", usesTigris: false},
		{backend: "webdav", want: ReplicaBackendWebDAV, replicaType: "webdav", usesTigris: false},
	}
	for _, tt := range tests {
		cfg := Config{ReplicaBackend: tt.backend}
		if got := cfg.ReplicaBackendName(); got != tt.want {
			t.Errorf("ReplicaBackendName(%q) = %q, want %q", tt.backend, got, tt.want)
		}
		if got := ReplicaTypeForBackend(tt.backend); got != tt.replicaType {
			t.Errorf("ReplicaTypeForBackend(%q) = %q, want %q", tt.backend, got, tt.replicaType)
		}
		if got := cfg.UsesTigrisReplica(); got != tt.usesTigris {
			t.Errorf("UsesTigrisReplica(%q) = %v, want %v", tt.backend, got, tt.usesTigris)
		}
	}
}

func TestReplicaBackendsListsEveryCoverageBackend(t *testing.T) {
	t.Parallel()

	got := map[string]bool{}
	for _, backend := range ReplicaBackends() {
		got[backend] = true
	}
	for _, want := range []string{ReplicaBackendTigris, ReplicaBackendMinIO, ReplicaBackendFile, ReplicaBackendSFTP, ReplicaBackendNATS, ReplicaBackendWebDAV} {
		if !got[want] {
			t.Errorf("ReplicaBackends() missing %q", want)
		}
	}
}

func TestReplicaBackendRoundTripsJSON(t *testing.T) {
	t.Parallel()

	parsed, err := ParseConfig(Config{ReplicaBackend: ReplicaBackendSFTP}.JSON())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ReplicaBackend != ReplicaBackendSFTP {
		t.Fatalf("ReplicaBackend = %q, want %q", parsed.ReplicaBackend, ReplicaBackendSFTP)
	}
	if body := (Config{}).JSON(); body != "{}" {
		t.Fatalf("default config JSON = %s, want {} so existing workers do not drift", body)
	}
}
