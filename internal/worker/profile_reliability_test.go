package worker

import (
	"context"
	"crypto/sha256"
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
		if incident.ID == profileIncidentSummaryID(snapshot.ProfileEpoch, "profile_capture_error", r.identity, 1, false) {
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
	if err != nil || len(entries) != profileIncidentsRetained+5 {
		t.Fatalf("durable events=%d err=%v, want three window openings and two closings once the interval has passed", len(entries), err)
	}
	snapshot := r.profilingSnapshot()
	if got := snapshot.ProfileIncidentsCoalesced["profile_capture_error"]; got != 3 {
		t.Fatalf("coalesced %d, want 3", got)
	}
	if len(snapshot.ProfileIncidents) != profileIncidentsRetained+1 {
		t.Fatalf("snapshot holds %d incidents, want the retained set plus only the open window", len(snapshot.ProfileIncidents))
	}
	windows := map[string]string{}
	for _, incident := range snapshot.ProfileIncidents {
		if strings.Contains(incident.ID, ":summary:") {
			windows[incident.ID] = incident.Message
		}
	}
	latest := profileIncidentSummaryID(snapshot.ProfileEpoch, "profile_capture_error", r.identity, 3, false)
	if len(windows) != 1 || !strings.HasPrefix(windows[latest], "3 profile_capture_error incidents coalesced") {
		t.Fatalf("open window missing: %v", windows)
	}
	for window := uint64(1); window <= 3; window++ {
		body := readOutboxEvent(t, cfg, profileIncidentSummaryID(snapshot.ProfileEpoch, "profile_capture_error", r.identity, window, false))
		if !strings.Contains(body, fmt.Sprintf("%d profile_capture_error incidents coalesced", window)) {
			t.Fatalf("window %d not persisted with its own id and count: %s", window, body)
		}
		if window == 3 {
			continue
		}
		closed := readOutboxEvent(t, cfg, profileIncidentSummaryID(snapshot.ProfileEpoch, "profile_capture_error", r.identity, window, true))
		if !strings.Contains(closed, fmt.Sprintf("%d profile_capture_error incidents coalesced", window)) {
			t.Fatalf("window %d not closed with its final count: %s", window, closed)
		}
	}
}

func readOutboxEvent(t *testing.T, cfg Config, id string) string {
	t.Helper()
	name := fmt.Sprintf("%x.json", sha256.Sum256([]byte(cfg.WorkerID+"\x00"+cfg.RunID+"\x00"+id)))
	body, err := os.ReadFile(filepath.Join(cfg.DataDir, "churn-outbox", name))
	if err != nil {
		t.Fatalf("outbox event %s: %v", id, err)
	}
	return string(body)
}

func TestProfileIncidentWindowIsClosedWithItsFinalCountAtShutdown(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	if err := r.finalizeProfileSummaries(); err != nil {
		t.Fatal(err)
	}
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	for i := 0; i < profileIncidentsRetained+40; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
	}
	epoch := r.profilingSnapshot().ProfileEpoch
	openID := profileIncidentSummaryID(epoch, "profile_capture_error", r.identity, 1, false)
	closedID := profileIncidentSummaryID(epoch, "profile_capture_error", r.identity, 1, true)
	if !strings.Contains(readOutboxEvent(t, cfg, openID), "1 profile_capture_error incidents coalesced") {
		t.Fatal("the window was not persisted when it opened")
	}

	if err := r.finalizeProfileSummaries(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(readOutboxEvent(t, cfg, closedID), "40 profile_capture_error incidents coalesced") {
		t.Fatal("the window was not closed with its final count")
	}
	snapshot := r.profilingSnapshot()
	ids := map[string]bool{}
	for _, incident := range snapshot.ProfileIncidents {
		if strings.Contains(incident.ID, ":summary:") {
			ids[incident.ID] = true
		}
	}
	if len(ids) != 1 || !ids[closedID] {
		t.Fatalf("snapshot summaries after shutdown = %v, want only the closed window", ids)
	}
	if err := r.finalizeProfileSummaries(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(cfg.DataDir, "churn-outbox"))
	if err != nil || len(entries) != profileIncidentsRetained+2 {
		t.Fatalf("durable events=%d err=%v, want one opening and one closing, with a second finalize a no-op", len(entries), err)
	}

	r.profiles.publishProfileStatus("periodic", "capture-failed")
	if !strings.Contains(readOutboxEvent(t, cfg, profileIncidentSummaryID(epoch, "profile_capture_error", r.identity, 2, false)), "41 profile_capture_error incidents coalesced") {
		t.Fatal("an incident after the close did not open a second window")
	}
}

func TestProfileSummaryFinalizeReportsAFailedClosingWrite(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	for i := 0; i < profileIncidentsRetained+5; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
	}
	if cause := context.Cause(ctx); cause != nil {
		t.Fatalf("run cancelled before the closing write: %v", cause)
	}
	outbox := filepath.Join(cfg.DataDir, "churn-outbox")
	if err := os.RemoveAll(outbox); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outbox, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := r.finalizeProfileSummaries()
	if err == nil || !strings.Contains(err.Error(), "persist profiling evidence") {
		t.Fatalf("finalize error = %v, want the failed closing write reported", err)
	}
	if r.profilingSnapshot().ProfileHistoryComplete {
		t.Fatal("a lost closing write left the history marked complete")
	}
	if err := r.finalizeProfileSummaries(); err != nil {
		t.Fatalf("second finalize = %v, want nothing left to close", err)
	}
}

func TestProfileIncidentSummaryIDsCarryWorkerAndMachine(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	for i := 0; i < profileIncidentsRetained; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
	}
	for _, machine := range []string{"machine-a", "machine-b"} {
		record := r.profiles.newRecord("hourly", "heap", machine+".pprof")
		record.RunID, record.MachineID, record.WorkerID = "run", machine, "worker"
		record.Error = "incomplete capture"
		r.profiles.onRecord(record)
	}
	snapshot := r.profilingSnapshot()
	ids := map[string]string{}
	for _, incident := range snapshot.ProfileIncidents {
		if strings.Contains(incident.ID, ":summary:") {
			ids[incident.ID] = incident.Run.MachineID
		}
	}
	if len(ids) != 2 {
		t.Fatalf("summaries = %v, want one per machine with its own id", ids)
	}
	for id, machine := range ids {
		if !strings.Contains(id, ":run:worker:"+machine+":") {
			t.Fatalf("summary id %s does not carry worker and machine %s", id, machine)
		}
	}
}

func TestProfileIncidentSummariesAreKeyedByRunIdentity(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "current", MachineID: "machine"}
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	for i := 0; i < profileIncidentsRetained; i++ {
		r.profiles.publishProfileStatus("periodic", "capture-failed")
	}
	earlier := r.profiles.newRecord("hourly", "heap", "earlier.pprof")
	earlier.RunID = "earlier"
	earlier.Error = "incomplete capture: bytes=0 copy=context deadline exceeded close=<nil>"
	r.profiles.onRecord(earlier)
	r.profiles.onRecord(earlier)
	r.profiles.publishProfileStatus("periodic", "capture-failed")
	r.profiles.publishProfileStatus("periodic", "capture-failed")

	snapshot := r.profilingSnapshot()
	byRun := map[string]string{}
	for _, incident := range snapshot.ProfileIncidents {
		if strings.Contains(incident.ID, ":summary:") {
			byRun[incident.Run.RunID] = incident.Message
		}
	}
	if len(byRun) != 2 || !strings.HasPrefix(byRun["earlier"], "1 profile_capture_error incidents coalesced") || !strings.HasPrefix(byRun["current"], "2 profile_capture_error incidents coalesced") {
		t.Fatalf("summaries by run = %v, want one per run identity with its own count", byRun)
	}
	if got := snapshot.ProfileIncidentsCoalesced["profile_capture_error"]; got != 3 {
		t.Fatalf("coalesced %d, want 3 across both runs", got)
	}
	if len(snapshot.ProfileIncidents) != profileIncidentsRetained+2 {
		t.Fatalf("snapshot holds %d incidents, want the retained set plus one summary per run", len(snapshot.ProfileIncidents))
	}
}

func TestProfileIncidentDedupeSetIsBounded(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), PprofCaptureEnabled: true, WorkerID: "worker", RunID: "run", MachineID: "machine"}
	r := NewRunner(cfg)
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r.observeProfiling(cancel)
	record := r.profiles.newRecord("hourly", "heap", "retry.pprof")
	record.Error = "incomplete capture"
	for attempt := 0; attempt < profileIncidentsSeenMax+100; attempt++ {
		record.addUploadFailure("artifact", errors.New("upload exited 75"))
		r.profiles.onRecord(record)
	}
	r.profileEvidence.mu.Lock()
	seen, order := len(r.profileEvidence.seen), len(r.profileEvidence.seenOrder)
	r.profileEvidence.mu.Unlock()
	if seen > profileIncidentsSeenMax || order > profileIncidentsSeenMax || seen != order {
		t.Fatalf("seen=%d order=%d, want at most %d", seen, order, profileIncidentsSeenMax)
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
