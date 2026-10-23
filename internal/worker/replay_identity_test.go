package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

func replayIdentityConfig(t *testing.T) Config {
	t.Helper()
	cfg := Config{
		DataDir: t.TempDir(), PprofCaptureEnabled: true, ControlBaseURL: "http://control.invalid",
		WorkerID: "worker", RunID: "run", MachineID: "machine", DeploymentID: 7, Source: "main",
		GitSHA: "soak", LitestreamSHA: "candidate", WorkloadID: "workload", WorkloadSHA: "generator",
		ProfileName: "gharchive", LoadMode: "replay", ReplayDataset: "gharchive", ReplaySpeed: 1,
		ReplayDataURL: "https://data.invalid/2025-01-01-0.json.gz",
	}
	dir := filepath.Join(cfg.DataDir, "datasets")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "2025-01-01-0.json.gz"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func expectRun(t *testing.T, identity reporting.WorkerIdentity) *model.DB {
	t.Helper()
	db, err := model.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.ExpectWorkerRun(identity); err != nil {
		t.Fatal(err)
	}
	return db
}

func outboxIdentities(t *testing.T, dataDir string) map[string]reporting.WorkerIdentity {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dataDir, "churn-outbox", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[string]reporting.WorkerIdentity)
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var event reporting.WorkerEventPayload
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatal(err)
		}
		identities[event.EventType] = event.WorkerIdentity
	}
	return identities
}

func TestReplaySetupKeepsEvidenceIdentityAttributed(t *testing.T) {
	r := NewRunner(replayIdentityConfig(t))
	expected := NewReporter(r.cfg).identity
	db := expectRun(t, expected)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeLogIncidents(cancel)
	r.observeProfiling(cancel)
	if err := r.prepareReplayData(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.cfg.ReplayDataPath == "" {
		t.Fatal("replay setup did not resolve a data path")
	}
	r.profiles.publishProfileStatus("baseline", "capture-failed")
	if _, err := r.litestreamLog.Write([]byte("level=ERROR msg=replay-failure\n")); err != nil {
		t.Fatal(err)
	}
	record := r.profiles.newRecord("baseline", "heap", "sample.pprof")
	if record.WorkloadHash != expected.ProfileHash {
		t.Fatalf("manifest workload hash = %s, want %s", record.WorkloadHash, expected.ProfileHash)
	}
	if manifest := string(record.WorkloadConfig); strings.Contains(manifest, "replay_data_path") || strings.Contains(manifest, "data.invalid") || !strings.Contains(manifest, `"replay_data_url":"redacted"`) {
		t.Fatalf("manifest workload config = %s", manifest)
	}
	dir := filepath.Join(r.cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	record.UploadAttempts = 1
	record.addUploadFailure("artifact", errors.New("upload exited 75"))
	r.profiles.saveRecord(context.Background(), filepath.Join(dir, "sample.pprof.json"), record)
	incidents := r.profilingSnapshot().ProfileIncidents
	if len(incidents) != 2 {
		t.Fatalf("incidents = %+v", incidents)
	}
	identities := outboxIdentities(t, r.cfg.DataDir)
	if len(identities) != 3 {
		t.Fatalf("outbox identities = %+v", identities)
	}
	for _, incident := range incidents {
		identities["profile_incident:"+incident.Kind] = incident.Run
	}
	identities["debug_snapshot"] = r.captureFailureDebugSnapshot("replay", nil, nil, 0).Run
	for name, identity := range identities {
		if identity != expected {
			t.Fatalf("%s identity diverged from reporter:\n got %+v\nwant %+v", name, identity, expected)
		}
		attributed, quarantined, err := db.ReportAttribution(identity)
		if err != nil || !attributed || quarantined {
			t.Fatalf("%s attribution = %v, %v, %v", name, attributed, quarantined, err)
		}
	}
}

func TestDifferentProfileConfigStillQuarantined(t *testing.T) {
	cfg := replayIdentityConfig(t)
	db := expectRun(t, NewReporter(cfg).identity)
	cfg.ReplaySpeed = 4
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	r.profiles.publishProfileStatus("baseline", "capture-failed")
	incidents := r.profilingSnapshot().ProfileIncidents
	if len(incidents) != 1 {
		t.Fatalf("incidents = %+v", incidents)
	}
	attributed, quarantined, err := db.ReportAttribution(incidents[0].Run)
	if err != nil || attributed || !quarantined {
		t.Fatalf("attribution = %v, %v, %v", attributed, quarantined, err)
	}
}
