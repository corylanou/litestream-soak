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
		{backend: "minio-sidecar", want: ReplicaBackendMinIOSidecar, replicaType: "s3", usesTigris: false},
		{backend: "file", want: ReplicaBackendFile, replicaType: "file", usesTigris: false},
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

func TestReplicaBackendRoundTripsJSON(t *testing.T) {
	t.Parallel()

	parsed, err := ParseConfig(Config{ReplicaBackend: ReplicaBackendMinIOSidecar}.JSON())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ReplicaBackend != ReplicaBackendMinIOSidecar {
		t.Fatalf("ReplicaBackend = %q, want %q", parsed.ReplicaBackend, ReplicaBackendMinIOSidecar)
	}
	if body := (Config{}).JSON(); body != "{}" {
		t.Fatalf("default config JSON = %s, want {} so existing workers do not drift", body)
	}
}
