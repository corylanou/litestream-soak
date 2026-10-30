package orchestrator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type gateFaultFixture struct {
	t    *testing.T
	db   *model.DB
	gate model.RCGate
	now  time.Time
}

func (f gateFaultFixture) workerID(source string) string {
	return "worker-" + source + "-" + gateLargeDBProfile
}

func (f gateFaultFixture) litestreamRan(source string) {
	f.t.Helper()
	if err := f.db.UpdateWorkerRuntimeSnapshot(f.workerID(source), reporting.RuntimePayload{
		LitestreamProcess:         reporting.ProcessObservation{Status: "fresh", PID: 42},
		LitestreamUptimeSeconds:   600,
		DBTXID:                    0x42,
		ReplicatedTXID:            0x42,
		DataDiskUsedPercent:       40,
		SnapshotCollectedAt:       f.gate.StartedAt.Add(20 * time.Minute),
		LitestreamSnapshotHealthy: true,
	}); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) litestreamNeverStarted(source string, diskUsedPercent float64) {
	f.t.Helper()
	if err := f.db.UpdateWorkerRuntimeSnapshot(f.workerID(source), reporting.RuntimePayload{
		LitestreamProcess:       reporting.ProcessObservation{Status: "unavailable", PID: 0},
		WorkerProcess:           reporting.ProcessObservation{Status: "fresh", PID: 7},
		DataDiskUsedPercent:     diskUsedPercent,
		SnapshotCollectedAt:     f.gate.StartedAt.Add(20 * time.Minute),
		LitestreamSnapshotError: "dial unix /data/litestream.sock: connect: no such file or directory",
	}); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) status(source string, status model.WorkerStatus, message string) {
	f.t.Helper()
	if err := f.db.UpdateWorkerStatus(f.workerID(source), status, message); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) event(source, eventType, message string, at time.Time) {
	f.t.Helper()
	if err := f.db.RecordEventAt(f.workerID(source), eventType, message, "", at); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) windowedEvent(source, eventType, message string, at time.Time) {
	f.t.Helper()
	if _, err := f.db.RecordWindowedEventAt(f.workerID(source), eventType, message, `{"line":"`+at.Format(time.RFC3339)+`"}`, at, platformEventCoalesceWindow); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) staleBurst(at time.Time) {
	f.t.Helper()
	for i, id := range []string{f.workerID(testGateCandidate), "worker-" + testGateCandidate + "-" + gateRemoteProfile, "worker-" + testGateBaseline + "-" + gateManyDB500Profile, "worker-" + testGateBaseline + "-" + gateRemoteProfile} {
		if err := f.db.RecordEventAt(id, "worker_stale", "Worker missed heartbeat deadline", "", at.Add(time.Duration(i)*time.Second)); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f gateFaultFixture) heartbeatNow(source string) {
	f.t.Helper()
	id := f.workerID(source)
	if err := f.db.UpdateWorkerHeartbeat(id); err != nil {
		f.t.Fatal(err)
	}
	worker, err := f.db.GetWorker(id)
	if err != nil {
		f.t.Fatal(err)
	}
	identity := reporting.WorkerIdentity{WorkerID: id, Name: id, Source: source, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, ProfileName: worker.ProfileName}
	if err := f.db.RecordRuntimeEvidence(identity, []byte(`{"uptime_seconds":1}`), true, "heartbeat"); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) flyDiskPressure(source, rawLine string, at time.Time) {
	f.t.Helper()
	details, err := json.Marshal(platformLogLine{Level: "error", Instance: f.workerID(source) + "-machine", Message: rawLine, Region: "ord", Timestamp: at})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.db.RecordWindowedEventAt(f.workerID(source), "platform_disk_full", "Fly log reported disk pressure: no space left on device", string(details), at, platformEventCoalesceWindow); err != nil {
		f.t.Fatal(err)
	}
}

func (f gateFaultFixture) harnessCapacityVerification(source string, at time.Time) {
	f.t.Helper()
	worker, err := f.db.GetWorker(f.workerID(source))
	if err != nil {
		f.t.Fatal(err)
	}
	deployment, err := f.db.GetDeploymentByVersion(source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		f.t.Fatalf("deployment for %s: %v", source, err)
	}
	completed := at
	mustRecordVerification(f.t, f.db, &model.Verification{
		WorkerID: worker.ID, StartedAt: at.Add(-5 * time.Minute), CompletedAt: &completed, Status: "aborted", CheckType: "integrity", Run: fixtureRun(*worker, *deployment), Attributed: true,
		ErrorMessage:          "restore failed: validation failed (exit 1): Error: decode database: write page 501132: write /data/scenarios/overload-truncate0/test.db.restored.tmp: no space left on device",
		FailureClassification: reporting.HarnessCapacityClassification("txid"),
	})
}

func (f gateFaultFixture) verification(source string, passed bool) {
	f.t.Helper()
	recordGateVerification(f.t, f.db, source, gateLargeDBProfile, "", passed, f.gate.StartedAt.Add(time.Hour))
}

func (f gateFaultFixture) assertPreStartSamplePersisted(source string) {
	f.t.Helper()
	worker, err := f.db.GetWorker(f.workerID(source))
	if err != nil {
		f.t.Fatal(err)
	}
	if worker.LastRuntimeAt != nil {
		f.t.Fatalf("last_runtime_at = %v, want NULL for a worker that never sent a healthy Litestream snapshot", worker.LastRuntimeAt)
	}
	runtime := parseRuntimeJSON(worker.LastRuntimeJSON, time.Time{})
	if runtime == nil || runtime.LitestreamProcess.PID != 0 || runtime.LitestreamSnapshotHealthy || runtime.LitestreamSnapshotError == "" || runtime.WorkerProcess.PID == 0 {
		f.t.Fatalf("persisted last_runtime_json does not carry the pre-start sample: %s", worker.LastRuntimeJSON)
	}
}

func TestRCGateWorkerFaultsScoreAsHardFailures(t *testing.T) {
	const (
		diskFullMessage = "Fly log reported disk pressure: no space left on device"
		staleMessage    = "Worker missed heartbeat deadline"
	)
	type side struct {
		status string
		reason string
	}
	tests := []struct {
		name          string
		arrange       func(f gateFaultFixture)
		candidate     side
		baseline      side
		verdict       string
		gateVerdict   string
		detailContain string
	}{
		{
			name: "degraded with disk error after litestream ran",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.status(testGateCandidate, model.WorkerDegraded, "Litestream disk-full signal: no space left on device")
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "no space left on device",
		},
		{
			name: "platform disk full event survives operator stop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(45*time.Minute))
				f.status(testGateCandidate, model.WorkerStopped, "")
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "no space left on device",
		},
		{
			name: "fly disk pressure on the verifier restore output is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.flyDiskPressure(testGateCandidate, `time=2026-10-30T02:36:12Z level=ERROR msg="Verification failed" error="restore failed: validation failed (exit 1): Error: decode database: write page 501132: write /data/scenarios/overload-truncate0/test.db.restored.tmp: no space left on device" duration=14m2s`, f.gate.StartedAt.Add(30*time.Minute))
				f.harnessCapacityVerification(testGateCandidate, f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "fly disk pressure on litestream staging stays a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.flyDiskPressure(testGateCandidate, `time=2026-10-30T02:36:12Z level=ERROR msg="disk full while staging ltx file" error="write /data/.test.db-litestream/ltx/0/0000000000000042.ltx: no space left on device"`, f.gate.StartedAt.Add(30*time.Minute))
				f.harnessCapacityVerification(testGateCandidate, f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "no space left on device",
		},
		{
			name: "path-less disk pressure beside a harness capacity verification is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.flyDiskPressure(testGateCandidate, "no space left on device", f.gate.StartedAt.Add(30*time.Minute))
				f.harnessCapacityVerification(testGateCandidate, f.gate.StartedAt.Add(36*time.Minute))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "path-less disk pressure without a harness capacity verification stays a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.flyDiskPressure(testGateCandidate, "no space left on device", f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "worker disk full no progress event",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, reporting.WorkerEventDiskFullNoProgress, "Litestream reported disk full and made no replication progress", f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "stale heartbeat event survives operator stop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(30*time.Minute))
				f.status(testGateCandidate, model.WorkerStopped, "")
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: staleMessage,
		},
		{
			name: "control-plane restart stale burst followed by recovery is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.staleBurst(f.now.Add(-12 * time.Minute))
				f.heartbeatNow(testGateCandidate)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "stale burst member that never recovered is a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.staleBurst(f.now.Add(-45 * time.Minute))
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.heartbeatNow(testGateBaseline)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "did not recover",
		},
		{
			name: "stale burst member that stayed silent past the grace period is a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.staleBurst(f.now.Add(-45 * time.Minute))
				f.heartbeatNow(testGateCandidate)
				f.heartbeatNow(testGateBaseline)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "heartbeats resumed after",
		},
		{
			name: "single stale that recovered within the grace period is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.now.Add(-10*time.Minute))
				f.heartbeatNow(testGateCandidate)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "single stale that stayed silent past the grace period is a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.now.Add(-45*time.Minute))
				f.heartbeatNow(testGateCandidate)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "heartbeats resumed after",
		},
		{
			name: "recent stale without recovery yet is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.now.Add(-5*time.Minute))
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "stale-degraded status while heartbeating is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.heartbeatNow(testGateCandidate)
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "degraded status without events",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "missed heartbeat",
		},
		{
			name: "degraded with full disk in last snapshot",
			arrange: func(f gateFaultFixture) {
				if err := f.db.UpdateWorkerRuntimeSnapshot(f.workerID(testGateCandidate), reporting.RuntimePayload{
					LitestreamProcess:         reporting.ProcessObservation{Status: "fresh", PID: 42},
					LitestreamUptimeSeconds:   600,
					DataDiskUsedPercent:       99.7,
					SnapshotCollectedAt:       f.gate.StartedAt.Add(20 * time.Minute),
					LitestreamSnapshotHealthy: true,
				}); err != nil {
					f.t.Fatal(err)
				}
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "99.7%",
		},
		{
			name: "repeated platform restarts are a crash loop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_restart", "Fly platform event: main child exited with exit code: 1", f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateCandidate, "platform_restart", "Fly platform event: main child exited with exit code: 1", f.gate.StartedAt.Add(50*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerCrashLoop},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "2 crash events",
		},
		{
			name: "crashes merged by the platform log window still count",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				const crash = "Fly platform event: main child exited with exit code: 1"
				f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(30*time.Minute))
				f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(33*time.Minute))
				f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(36*time.Minute))
				events, err := f.db.ListWorkerEvents(f.workerID(testGateCandidate), 10)
				if err != nil || len(events) != 1 || events[0].Occurrences != 3 {
					f.t.Fatalf("windowed crash events = %+v, %v; want one row with 3 occurrences", events, err)
				}
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerCrashLoop},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "3 crash events",
		},
		{
			name: "a replayed single crash is not a crash loop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				const crash = "Fly platform event: main child exited with exit code: 1"
				for range 3 {
					f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(30*time.Minute))
				}
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "a legacy crash row replayed after migration is not a crash loop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				const crash = "Fly platform event: main child exited with exit code: 1"
				at := f.gate.StartedAt.Add(30 * time.Minute)
				details := `{"line":"` + at.Format(time.RFC3339) + `"}`
				if err := f.db.RecordEventAt(f.workerID(testGateCandidate), "platform_restart", crash, details, at); err != nil {
					f.t.Fatal(err)
				}
				for range 2 {
					f.windowedEvent(testGateCandidate, "platform_restart", crash, at)
				}
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "replayed batch with two distinct crashes counts two",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				const crash = "Fly platform event: main child exited with exit code: 1"
				for range 2 {
					f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(30*time.Minute))
					f.windowedEvent(testGateCandidate, "platform_restart", crash, f.gate.StartedAt.Add(33*time.Minute))
				}
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideFailing, rcGateReasonWorkerCrashLoop},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictRegression,
			gateVerdict:   rcGateVerdictFail,
			detailContain: "2 crash events",
		},
		{
			name: "fly giving up on restarts is a crash loop",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_restart", "Fly platform event: machine exited with exit code 1, not restarting", f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonWorkerCrashLoop},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "single restart is not a fault",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_restart", "Fly platform event: machine restarted by platform", f.gate.StartedAt.Add(30*time.Minute))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "events before the gate window are ignored",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(-time.Minute))
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(-2*time.Minute))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "events after the gate window are ignored",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.now.Add(time.Hour))
				f.verification(testGateCandidate, true)
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "fault overrides earlier passes",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.verification(testGateCandidate, true)
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "both sides exhausted disk",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.litestreamRan(testGateBaseline)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateBaseline, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(35*time.Minute))
			},
			candidate:   side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:    side{rcGateSideFailing, rcGateReasonDiskExhausted},
			verdict:     rcGateVerdictBothFailing,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "baseline fault does not fail the candidate",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateBaseline)
				f.status(testGateBaseline, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateCandidate, true)
			},
			candidate:   side{rcGateSidePassing, ""},
			baseline:    side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			verdict:     rcGateVerdictPass,
			gateVerdict: rcGateVerdictIncomplete,
		},
		{
			name: "disk full after litestream ran but before any verification",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(30*time.Minute))
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "disk full before litestream started is harness inconclusive",
			arrange: func(f gateFaultFixture) {
				f.litestreamNeverStarted(testGateCandidate, 100)
				f.event(testGateCandidate, "platform_disk_full", diskFullMessage, f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateCandidate, "platform_restart", "Fly platform event: main child exited with exit code: 1", f.gate.StartedAt.Add(31*time.Minute))
				f.event(testGateCandidate, "platform_restart", "Fly platform event: machine exited with exit code 1, not restarting", f.gate.StartedAt.Add(40*time.Minute))
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(55*time.Minute))
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.assertPreStartSamplePersisted(testGateCandidate)
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideNoCoverage, rcGateReasonHarnessInconclusive},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictIncomplete,
			gateVerdict:   rcGateVerdictIncomplete,
			detailContain: "disk_exhausted before litestream started (last heartbeat reported litestream pid 0 with no ipc snapshot",
		},
		{
			name: "populate disk fill without fly logs is harness inconclusive",
			arrange: func(f gateFaultFixture) {
				f.litestreamNeverStarted(testGateCandidate, 99.9)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(55*time.Minute))
				f.status(testGateCandidate, model.WorkerStopped, "")
				f.assertPreStartSamplePersisted(testGateCandidate)
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideNoCoverage, rcGateReasonHarnessInconclusive},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictIncomplete,
			gateVerdict:   rcGateVerdictIncomplete,
			detailContain: "data disk 99.9% used",
		},
		{
			name: "pre-start fatal event is harness inconclusive without a sample",
			arrange: func(f gateFaultFixture) {
				f.event(testGateCandidate, "worker_failed", "populate: write batch: write /data/test.db-wal: no space left on device", f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateCandidate, "worker_failed", "populate: write batch: write /data/test.db-wal: no space left on device", f.gate.StartedAt.Add(45*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:     side{rcGateSideNoCoverage, rcGateReasonHarnessInconclusive},
			baseline:      side{rcGateSidePassing, ""},
			verdict:       rcGateVerdictIncomplete,
			gateVerdict:   rcGateVerdictIncomplete,
			detailContain: "worker_crash_loop before litestream started (worker_failed event populate)",
		},
		{
			name: "fault without pre-start evidence is a hard failure",
			arrange: func(f gateFaultFixture) {
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonWorkerDegraded},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "pid 0 sample after a healthy snapshot is a hard failure",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.litestreamNeverStarted(testGateCandidate, 100)
				f.event(testGateCandidate, "worker_stale", staleMessage, f.gate.StartedAt.Add(55*time.Minute))
				f.status(testGateCandidate, model.WorkerDegraded, "worker missed heartbeat deadline")
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonDiskExhausted},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
		{
			name: "post-start crash with a late unhealthy sample is a hard failure",
			arrange: func(f gateFaultFixture) {
				f.litestreamRan(testGateCandidate)
				f.event(testGateCandidate, "worker_failed", "litestream exited: signal: killed", f.gate.StartedAt.Add(30*time.Minute))
				f.event(testGateCandidate, "worker_failed", "litestream exited: signal: killed", f.gate.StartedAt.Add(45*time.Minute))
				f.verification(testGateBaseline, true)
			},
			candidate:   side{rcGateSideFailing, rcGateReasonWorkerCrashLoop},
			baseline:    side{rcGateSidePassing, ""},
			verdict:     rcGateVerdictRegression,
			gateVerdict: rcGateVerdictFail,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			now := time.Now().UTC()
			gate := registerTestGate(t, db, false, now.Add(-2*time.Hour))
			createTestGateFleet(t, db, gate)
			tt.arrange(gateFaultFixture{t: t, db: db, gate: gate, now: now})

			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := buildRCGateLedgerEntry(db, *stored, now)
			if err != nil {
				t.Fatal(err)
			}
			var scenario RCGateScenarioResult
			for _, candidate := range entry.Scenarios {
				if candidate.Scenario == "large-db" {
					scenario = candidate
				}
			}
			check := func(label string, got RCGateSideResult, want side) {
				t.Helper()
				if got.Status != want.status || got.Reason != want.reason {
					t.Fatalf("%s = status %q reason %q, want %q/%q (%+v)", label, got.Status, got.Reason, want.status, want.reason, got)
				}
				if want.reason != "" && got.ReasonDetail == "" {
					t.Fatalf("%s reason %q has no detail", label, want.reason)
				}
			}
			check("candidate", scenario.Candidate, tt.candidate)
			check("baseline", scenario.Baseline, tt.baseline)
			if tt.detailContain != "" && !containsFold(scenario.Candidate.ReasonDetail, tt.detailContain) {
				t.Fatalf("candidate detail = %q, want it to contain %q", scenario.Candidate.ReasonDetail, tt.detailContain)
			}
			if scenario.Verdict != tt.verdict {
				t.Fatalf("large-db verdict = %q, want %q (%+v)", scenario.Verdict, tt.verdict, scenario)
			}
			if len(scenario.Modes) == 0 {
				t.Fatalf("large-db has no mode results: %+v", scenario)
			}
			for _, mode := range scenario.Modes {
				if mode.Candidate.Reason != tt.candidate.reason || mode.Baseline.Reason != tt.baseline.reason {
					t.Fatalf("mode %s reasons = %q/%q, want %q/%q", mode.Mode, mode.Candidate.Reason, mode.Baseline.Reason, tt.candidate.reason, tt.baseline.reason)
				}
				if mode.Verdict != tt.verdict {
					t.Fatalf("mode %s verdict = %q, want %q (%+v)", mode.Mode, mode.Verdict, tt.verdict, mode)
				}
			}
			if entry.Verdict != tt.gateVerdict {
				t.Fatalf("gate verdict = %q, want %q", entry.Verdict, tt.gateVerdict)
			}
		})
	}
}

func TestUpgradeGateWorkerFaultsUseTheSameRules(t *testing.T) {
	type verification struct {
		source, profile, phase, checkType, mode string
	}
	var coverage []verification
	for _, phase := range []string{upgradePhasePre, upgradePhasePost} {
		for _, mode := range gateRotatingRestoreModes {
			coverage = append(coverage, verification{testUpgradeCandidate, gateUpgradeLaneProfile, phase, "integrity", mode})
		}
	}
	coverage = append(coverage,
		verification{testUpgradeCandidate, gateUpgradeLaneProfile, upgradePhasePost, gateCheckFollow, gateModeFollow},
		verification{testUpgradeCandidate, gateUpgradeLaneProfile, upgradePhasePost, gateCheckHistorical, "txid"},
	)
	for _, source := range []string{testUpgradeCandidate, testUpgradeBaseline} {
		for _, mode := range gateRotatingRestoreModes {
			coverage = append(coverage, verification{source, gateUpgradeFollowProfile, "", "integrity", mode})
		}
		coverage = append(coverage, verification{source, gateUpgradeFollowProfile, "", gateCheckFollow, gateModeFollow})
	}
	tests := []struct {
		name          string
		faultProfile  string
		wantScenarios map[string]string
		wantReason    map[string]string
	}{
		{
			name:          "candidate follow worker disk exhaustion is a follow regression",
			faultProfile:  gateUpgradeFollowProfile,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictRegression},
			wantReason:    map[string]string{"follow-l9": rcGateReasonDiskExhausted},
		},
		{
			name:          "lane worker disk exhaustion fails every lane scenario",
			faultProfile:  gateUpgradeLaneProfile,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictBothFailing, "upgrade-lane-historical": rcGateVerdictBothFailing, "upgrade-lane-follow": rcGateVerdictRegression, "follow-l9": rcGateVerdictPass},
			wantReason:    map[string]string{"upgrade-lane": rcGateReasonDiskExhausted, "upgrade-lane-historical": rcGateReasonDiskExhausted, "upgrade-lane-follow": rcGateReasonDiskExhausted},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestUpgradeGate(t, db, "", time.Now().UTC().Add(-3*time.Hour))
			createTestGateFleet(t, db, gate)
			inside := gate.StartedAt.Add(time.Hour)
			for i, v := range coverage {
				recordUpgradeGateVerification(t, db, v.source, v.profile, v.phase, "", v.checkType, v.mode, "passed", inside.Add(time.Duration(i)*time.Minute))
			}
			if err := db.RecordEventAt("worker-"+testUpgradeCandidate+"-"+tt.faultProfile, "platform_disk_full", "Fly log reported disk pressure: no space left on device", "", inside.Add(2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			results, err := buildRCGateResults(db, *stored, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if results.Verdict != rcGateVerdictFail || len(results.Scenarios) != len(tt.wantScenarios) {
				t.Fatalf("verdict = %s scenarios = %+v", results.Verdict, results.Scenarios)
			}
			for _, scenario := range results.Scenarios {
				if want := tt.wantScenarios[scenario.Scenario]; scenario.Verdict != want {
					t.Fatalf("%s verdict = %s, want %s: %+v", scenario.Scenario, scenario.Verdict, want, scenario)
				}
				if want := tt.wantReason[scenario.Scenario]; scenario.Candidate.Reason != want {
					t.Fatalf("%s candidate reason = %q, want %q: %+v", scenario.Scenario, scenario.Candidate.Reason, want, scenario.Candidate)
				}
			}
		})
	}
}

func TestRCGateStressPlannedPhaseRestartsAreNotCrashes(t *testing.T) {
	phase, err := time.ParseDuration(rcGateStressPhase)
	if err != nil {
		t.Fatal(err)
	}
	const (
		phaseEnded = "stress scenario phase ended"
		exitOne    = "Fly platform event: main child exited with exit code: 1"
	)
	tests := []struct {
		name       string
		arrange    func(db *model.DB, workerID string, boundary time.Time)
		wantStatus string
		wantReason string
	}{
		{
			name: "phase-end worker_failed plus the restart it causes",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				for _, at := range []time.Time{boundary.Add(15 * time.Second), boundary.Add(15*time.Second + time.Millisecond)} {
					if err := db.RecordEventAt(workerID, "worker_failed", phaseEnded, "", at); err != nil {
						t.Fatal(err)
					}
				}
				for _, at := range []time.Time{boundary.Add(40 * time.Second), boundary.Add(95 * time.Second)} {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", exitOne, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantStatus: rcGateSidePassing,
		},
		{
			name: "clean exit at the wall-clock phase boundary without a planned event",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				record := func(at time.Time, message string) {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", message, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
				record(boundary.Add(20*time.Second), "Fly platform event: main child exited with exit code: 0")
				record(boundary.Add(-3*time.Hour), exitOne)
			},
			wantStatus: rcGateSidePassing,
		},
		{
			name: "unclean restarts at the boundary without a planned event are a crash loop",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				for _, at := range []time.Time{boundary.Add(-30 * time.Second), boundary.Add(2 * time.Minute)} {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", exitOne, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
		{
			name: "genuine crash loop after the planned phase restart",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				if err := db.RecordEventAt(workerID, "worker_failed", phaseEnded, "", boundary.Add(15*time.Second)); err != nil {
					t.Fatal(err)
				}
				for _, at := range []time.Time{boundary.Add(40 * time.Second), boundary.Add(3 * time.Minute), boundary.Add(6 * time.Minute)} {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", exitOne, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
		{
			name: "duplicate lifecycle reports share one allowance per boundary",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				for _, at := range []time.Time{boundary.Add(15 * time.Second), boundary.Add(15*time.Second + time.Millisecond), boundary.Add(16 * time.Second)} {
					if err := db.RecordEventAt(workerID, "worker_failed", phaseEnded, "", at); err != nil {
						t.Fatal(err)
					}
				}
				record := func(at time.Time, message string) {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", message, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
				record(boundary.Add(20*time.Second), "Fly platform event: main child exited with exit code: 0")
				record(boundary.Add(40*time.Second), exitOne)
				record(boundary.Add(4*time.Minute), "Fly platform event: main child exited with exit code: 2")
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
		{
			name: "terminal exit near the boundary still counts",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				if err := db.RecordEventAt(workerID, "worker_failed", phaseEnded, "", boundary.Add(15*time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", "Fly platform event: machine exited with exit code 1, not restarting", `{"line":"terminal"}`, boundary.Add(time.Minute), platformEventCoalesceWindow); err != nil {
					t.Fatal(err)
				}
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
		{
			name: "restarts away from any boundary stay a crash loop",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				for _, at := range []time.Time{boundary.Add(-3 * time.Hour), boundary.Add(-2*time.Hour - 30*time.Minute)} {
					if _, err := db.RecordWindowedEventAt(workerID, "platform_restart", exitOne, `{"line":"`+at.Format(time.RFC3339Nano)+`"}`, at, platformEventCoalesceWindow); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
		{
			name: "a real worker_failed near the boundary still counts",
			arrange: func(db *model.DB, workerID string, boundary time.Time) {
				for _, at := range []time.Time{boundary.Add(20 * time.Second), boundary.Add(4 * time.Minute)} {
					if err := db.RecordEventAt(workerID, "worker_failed", "wait for first sync: litestream exited: exit status 2", "", at); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantStatus: rcGateSideFailing,
			wantReason: rcGateReasonWorkerCrashLoop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			now := time.Now().UTC()
			gate := registerTestGate(t, db, false, now.Add(-10*time.Hour))
			createTestGateFleet(t, db, gate)
			boundary := now.Add(-time.Hour).Truncate(phase)
			if boundary.Before(gate.StartedAt.Add(4 * time.Hour)) {
				boundary = boundary.Add(phase)
			}
			if boundary.After(now) || boundary.Before(gate.StartedAt) {
				t.Fatalf("no usable phase boundary between %s and %s", gate.StartedAt, now)
			}
			workerID := "worker-" + testGateCandidate + "-" + gateStressProfile
			tt.arrange(db, workerID, boundary)
			for _, scenario := range []string{"overload-truncate0", "pinned-reader"} {
				recordGateVerification(t, db, testGateCandidate, gateStressProfile, scenario, true, gate.StartedAt.Add(time.Hour))
				recordGateVerification(t, db, testGateBaseline, gateStressProfile, scenario, true, gate.StartedAt.Add(time.Hour))
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := buildRCGateLedgerEntry(db, *stored, now)
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			for _, scenario := range entry.Scenarios {
				if scenario.Profile != gateStressProfile {
					continue
				}
				seen++
				if scenario.Candidate.Status != tt.wantStatus || scenario.Candidate.Reason != tt.wantReason {
					t.Fatalf("%s candidate = %+v, want %s/%q", scenario.Scenario, scenario.Candidate, tt.wantStatus, tt.wantReason)
				}
			}
			if seen != 2 {
				t.Fatalf("stress scenarios = %d", seen)
			}
		})
	}
}

func TestRCGateStressFaultAppliesToEveryScenarioOnTheWorker(t *testing.T) {
	db := openTestDB(t)
	now := time.Now().UTC()
	gate := registerTestGate(t, db, false, now.Add(-2*time.Hour))
	createTestGateFleet(t, db, gate)
	at := gate.StartedAt.Add(time.Hour)
	for _, scenario := range []string{"overload-truncate0", "pinned-reader"} {
		recordGateVerification(t, db, testGateCandidate, gateStressProfile, scenario, true, at)
		recordGateVerification(t, db, testGateBaseline, gateStressProfile, scenario, true, at)
	}
	if err := db.RecordEventAt("worker-"+testGateCandidate+"-"+gateStressProfile, "platform_disk_full", "Fly log reported disk pressure: no space left on device", "", at.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	stored, err := db.GetRCGate(gate.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := buildRCGateLedgerEntry(db, *stored, now)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, scenario := range entry.Scenarios {
		if scenario.Profile != gateStressProfile {
			continue
		}
		seen++
		if scenario.Candidate.Status != rcGateSideFailing || scenario.Candidate.Reason != rcGateReasonDiskExhausted || scenario.Candidate.Passed != 1 {
			t.Fatalf("%s candidate = %+v", scenario.Scenario, scenario.Candidate)
		}
		if scenario.Verdict != rcGateVerdictRegression {
			t.Fatalf("%s verdict = %q", scenario.Scenario, scenario.Verdict)
		}
	}
	if seen != 2 || entry.Verdict != rcGateVerdictFail {
		t.Fatalf("stress scenarios = %d gate verdict = %q", seen, entry.Verdict)
	}
}
