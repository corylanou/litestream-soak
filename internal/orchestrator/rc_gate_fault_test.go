package orchestrator

import (
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
