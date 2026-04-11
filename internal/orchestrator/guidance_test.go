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
	if diagnosis.Confidence != "high" {
		t.Fatalf("confidence=%q want %q", diagnosis.Confidence, "high")
	}
	if len(diagnosis.Clusters) != 1 {
		t.Fatalf("clusters=%d want 1", len(diagnosis.Clusters))
	}
}

func TestBuildDiagnosisSnapshotClustersMultipleFailureFamilies(t *testing.T) {
	summaries := []WorkerSummaryResponse{
		{
			Worker: model.Worker{ID: "w1", ProfileName: "gharchive-replay", Status: model.WorkerDegraded, Name: "worker-main-gharchive"},
			Workload: workload.Config{
				LoadMode:      "replay",
				ReplayDataset: "gharchive",
			},
			LastVerification:         &model.Verification{WorkerID: "w1"},
			CurrentFailureStage:      "sync",
			CurrentFailureSignature:  "litestream_sync_socket_refused",
			CurrentProbableSubsystem: "Litestream sync/control socket",
		},
		{
			Worker: model.Worker{ID: "w2", ProfileName: "high-volume", Status: model.WorkerDegraded, Name: "worker-main-high-vol"},
			Workload: workload.Config{
				LoadMode: "synthetic",
				Pattern:  "wave",
			},
			LastVerification:         &model.Verification{WorkerID: "w2"},
			CurrentFailureStage:      "sync",
			CurrentFailureSignature:  "litestream_sync_socket_refused",
			CurrentProbableSubsystem: "Litestream sync/control socket",
		},
		{
			Worker: model.Worker{ID: "w3", ProfileName: "taxi-replay", Status: model.WorkerDegraded, Name: "worker-main-taxi-replay"},
			Workload: workload.Config{
				LoadMode:      "replay",
				ReplayDataset: "taxi",
			},
			LastVerification:         &model.Verification{WorkerID: "w3"},
			CurrentFailureStage:      "restore",
			CurrentFailureSignature:  "replica_s3_timeout",
			CurrentProbableSubsystem: "Replication or restore path",
		},
	}

	diagnosis := buildDiagnosisSnapshot(summaries)
	if len(diagnosis.Clusters) != 2 {
		t.Fatalf("clusters=%d want 2", len(diagnosis.Clusters))
	}
	if diagnosis.Headline != "2 active failure clusters across 3 workers" {
		t.Fatalf("headline=%q", diagnosis.Headline)
	}
	if diagnosis.Clusters[0].Signature != "litestream_sync_socket_refused" {
		t.Fatalf("top cluster signature=%q", diagnosis.Clusters[0].Signature)
	}
	if diagnosis.Clusters[0].Confidence != "high" {
		t.Fatalf("top cluster confidence=%q", diagnosis.Clusters[0].Confidence)
	}
}
