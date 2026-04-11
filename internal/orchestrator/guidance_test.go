package orchestrator

import (
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestInferFailureStageSync(t *testing.T) {
	verification := &model.Verification{
		CheckType:    "integrity",
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	}

	if got := inferFailureStage(verification); got != "sync" {
		t.Fatalf("inferFailureStage()=%q want %q", got, "sync")
	}
}

func TestInferFailureSignatureSyncSocketRefused(t *testing.T) {
	verification := &model.Verification{
		CheckType:    "integrity",
		ErrorMessage: `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`,
	}

	if got := inferFailureSignature(verification); got != "litestream_sync_socket_refused" {
		t.Fatalf("inferFailureSignature()=%q want %q", got, "litestream_sync_socket_refused")
	}
}

func TestBuildIncidentGuideSync(t *testing.T) {
	bundle := &IncidentBundle{
		Worker: model.Worker{
			ID:     "worker-main-gharchive",
			Status: model.WorkerDegraded,
		},
		ActiveFailure:    true,
		FailureStage:     "sync",
		FailureSignature: "litestream_sync_socket_refused",
		Workload: workload.Config{
			LoadMode:      "replay",
			ReplayDataset: "gharchive",
		},
		TriageCommands: []string{"fly logs -a litestream-soak -i machine"},
	}

	guide := buildIncidentGuide(bundle)
	if guide.ProbableSubsystem != "Litestream sync/control socket" {
		t.Fatalf("guide probable subsystem=%q", guide.ProbableSubsystem)
	}
	if guide.RecommendedPromptMode != "litestream" {
		t.Fatalf("guide recommended prompt=%q", guide.RecommendedPromptMode)
	}
	if len(guide.NextSteps) == 0 {
		t.Fatal("expected next steps")
	}
}

func TestBuildDiagnosisSnapshotUsesCurrentFailures(t *testing.T) {
	summaries := []WorkerSummaryResponse{
		{
			Worker: model.Worker{ID: "w1", ProfileName: "gharchive", Status: model.WorkerDegraded},
			Workload: workload.Config{
				LoadMode:      "replay",
				ReplayDataset: "gharchive",
			},
			CurrentFailureStage:      "sync",
			CurrentFailureSignature:  "litestream_sync_socket_refused",
			CurrentProbableSubsystem: "Litestream sync/control socket",
		},
		{
			Worker: model.Worker{ID: "w2", ProfileName: "high-volume", Status: model.WorkerDegraded},
			Workload: workload.Config{
				LoadMode: "synthetic",
				Pattern:  "wave",
			},
			CurrentFailureStage:      "sync",
			CurrentFailureSignature:  "litestream_sync_socket_refused",
			CurrentProbableSubsystem: "Litestream sync/control socket",
		},
	}

	diagnosis := buildDiagnosisSnapshot(summaries)
	if diagnosis.AffectedWorkers != 2 {
		t.Fatalf("affected workers=%d want 2", diagnosis.AffectedWorkers)
	}
	if diagnosis.ProbableSubsystem != "Litestream sync/control socket" {
		t.Fatalf("probable subsystem=%q", diagnosis.ProbableSubsystem)
	}
	if diagnosis.DominantSignature != "litestream_sync_socket_refused" {
		t.Fatalf("dominant signature=%q", diagnosis.DominantSignature)
	}
}
