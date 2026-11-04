package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProfileSnapshotRetainsUploadFailureAfterRecovery(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	dir := filepath.Join(cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	record := r.profiles.newRecord("baseline", "heap", "sample.pprof")
	record.Status = "available"
	record.UploadAttempts = 1
	record.addUploadFailure("artifact", errors.New("upload exited 75"))
	record.Upload = "uploaded"
	record.UploadError = ""
	r.profiles.saveRecord(context.Background(), filepath.Join(dir, "sample.pprof.json"), record)
	raw, _ := json.Marshal(r.currentSnapshot())
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot["profile_records"]) == 0 {
		t.Fatal("profiling history missing from heartbeat snapshot")
	}
	var records []profileRecord
	if err := json.Unmarshal(snapshot["profile_records"], &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].UploadFailureCount != 1 || records[0].Upload != "uploaded" {
		t.Fatalf("historical upload failure lost: %+v", records)
	}
}

func TestProfileObserverRetainsDiagnosticAfterManifestPruning(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "original", MachineID: "machine"}
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	dir := filepath.Join(cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	record := r.profiles.newRecord("baseline", "heap", "sample.pprof")
	record.Status = "available"
	record.UploadAttempts = 1
	record.addUploadFailure("artifact", errors.New("upload exited 75"))
	filename := filepath.Join(dir, "sample.pprof.json")
	r.profiles.saveRecord(context.Background(), filename, record)
	record.Upload = "uploaded"
	record.UploadError = ""
	r.profiles.saveRecord(context.Background(), filename, record)
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	snapshot := r.currentSnapshot()
	if len(snapshot.ProfileIncidents) != 1 || snapshot.ProfileIncidents[0].Run.RunID != "original" || !strings.Contains(snapshot.ProfileIncidents[0].Message, "exited 75") {
		t.Fatalf("diagnostic lost: %+v", snapshot.ProfilingEvidence)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "churn-outbox"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("durable event=%v %v", entries, err)
	}
}

func TestProfilePersistenceFailureStopsRun(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run"}
	r := NewRunner(cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "churn-outbox"), []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	r.profiles.publishProfileStatus("baseline", "capture-failed")
	if context.Cause(ctx) == nil || r.profilingSnapshot().ProfileHistoryComplete {
		t.Fatal("failed durable profiling evidence did not stop workload")
	}
}

func TestNeutralProfileStatusRingPreservesCompleteObservation(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	dir := filepath.Join(cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	record := r.profiles.newRecord("baseline", "heap", "sample.pprof")
	record.Status = "available"
	r.profiles.saveRecord(context.Background(), filepath.Join(dir, "sample.pprof.json"), record)
	for i := 0; i < 80; i++ {
		r.profiles.recordStatus("periodic", "cpu-rate-limited")
		r.profiles.recordStatus("periodic", "block-disabled")
	}
	snapshot := r.profilingSnapshot()
	if !snapshot.ProfileHistoryComplete || snapshot.ProfileCapability != "observed" || len(snapshot.ProfileIncidents) != 0 {
		t.Fatalf("neutral ring invalidated coverage: %+v", snapshot)
	}
}

func TestProfileIncidentFloodNeverStopsRun(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	const flood = 3000
	for i := 0; i < flood; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
	}
	if cause := context.Cause(ctx); cause != nil {
		t.Fatalf("profiling incidents stopped the run: %v", cause)
	}
	snapshot := r.profilingSnapshot()
	if !snapshot.ProfileHistoryComplete || snapshot.ProfileEvidenceDropped != 0 {
		t.Fatalf("coalesced incidents marked the history incomplete: %+v", snapshot)
	}
	if len(snapshot.ProfileIncidents) != profileIncidentsRetained+1 {
		t.Fatalf("retained %d incidents, want %d plus one summary", len(snapshot.ProfileIncidents), profileIncidentsRetained)
	}
	if got := snapshot.ProfileIncidentsCoalesced["profile_capture_error"]; got != flood-profileIncidentsRetained {
		t.Fatalf("coalesced %d, want %d", got, flood-profileIncidentsRetained)
	}
	var summary string
	for _, incident := range snapshot.ProfileIncidents {
		if incident.ID == profileIncidentSummaryID(snapshot.ProfileEpoch, "profile_capture_error") {
			summary = incident.Message
		}
	}
	if !strings.Contains(summary, fmt.Sprintf("%d profile_capture_error incidents coalesced since", flood-profileIncidentsRetained)) || !strings.Contains(summary, "periodic: capture-failed") {
		t.Fatalf("summary incident missing or wrong: %q", summary)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "churn-outbox"))
	if err != nil || len(entries) != profileIncidentsRetained+1 {
		t.Fatalf("durable events=%d err=%v, want %d retained plus one summary", len(entries), err, profileIncidentsRetained+1)
	}
}

func TestProfileIncidentSummaryIsPersistedAgainAfterTheInterval(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	r.profileEvidence.summaryInterval = time.Nanosecond
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	for i := 0; i < profileIncidentsRetained+3; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
		time.Sleep(time.Microsecond)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "churn-outbox"))
	if err != nil || len(entries) != profileIncidentsRetained+3 {
		t.Fatalf("durable events=%d err=%v, want one summary per coalesced incident once the interval has passed", len(entries), err)
	}
	if got := r.profilingSnapshot().ProfileIncidentsCoalesced["profile_capture_error"]; got != 3 {
		t.Fatalf("coalesced %d, want 3", got)
	}
}

func TestProfileIncidentsSurviveAFullOutboxWithoutStoppingTheRun(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	dir := filepath.Join(cfg.DataDir, "churn-outbox")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < evidenceOutboxMaxEvents; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d.json", i)), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r.profiles.publishProfileStatus("periodic", "capture-failed")
	if cause := context.Cause(ctx); cause != nil {
		t.Fatalf("a full outbox stopped the run: %v", cause)
	}
	snapshot := r.profilingSnapshot()
	if snapshot.ProfileEvidenceDropped != 1 || len(snapshot.ProfileIncidents) != 1 || !snapshot.ProfileHistoryComplete {
		t.Fatalf("dropped=%d incidents=%d complete=%v, want the incident kept in the snapshot and one drop counted", snapshot.ProfileEvidenceDropped, len(snapshot.ProfileIncidents), snapshot.ProfileHistoryComplete)
	}
}
