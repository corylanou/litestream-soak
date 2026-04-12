package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
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
		ReportedRuntime: &reporting.RuntimePayload{
			SnapshotCollectedAt:       timeMustParse("2026-04-11T21:05:00Z"),
			LitestreamSnapshotHealthy: false,
			LitestreamSnapshotError:   "read txid: dial unix /data/litestream.sock: connect: connection refused",
		},
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
	if !strings.Contains(strings.Join(guide.WhyLikely, "\n"), "runtime snapshot is unhealthy") {
		t.Fatalf("expected runtime snapshot note in why_likely, got %v", guide.WhyLikely)
	}
}

func TestBuildIncidentGuideLegacyRuntimeTelemetry(t *testing.T) {
	bundle := &IncidentBundle{
		Worker: model.Worker{
			ID:     "worker-main-burst-vol",
			Status: model.WorkerDegraded,
		},
		ActiveFailure:    true,
		FailureStage:     "sync",
		FailureSignature: "litestream_sync_socket_refused",
		ReportedRuntime: &reporting.RuntimePayload{
			SnapshotCollectedAt:       timeMustParse("2026-04-12T01:40:51Z"),
			DBStatus:                  "replicating",
			LitestreamSnapshotHealthy: false,
			LitestreamSnapshotError:   reporting.LegacyRuntimeTelemetryError,
		},
		Workload: workload.Config{
			LoadMode: "synthetic",
			Pattern:  "burst",
		},
	}

	guide := buildIncidentGuide(bundle)
	why := strings.Join(guide.WhyLikely, "\n")
	steps := strings.Join(guide.NextSteps, "\n")
	if !strings.Contains(why, "legacy runtime telemetry") {
		t.Fatalf("expected legacy runtime note in why_likely, got %v", guide.WhyLikely)
	}
	if !strings.Contains(steps, "Treat runtime fields on this page as advisory") {
		t.Fatalf("expected legacy runtime note in next_steps, got %v", guide.NextSteps)
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
			RuntimeSnapshotStatus:    reporting.RuntimeSnapshotStatusLegacy,
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
			RuntimeSnapshotStatus:    reporting.RuntimeSnapshotStatusLegacy,
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
			RuntimeSnapshotStatus:    reporting.RuntimeSnapshotStatusHealthy,
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
	if !strings.Contains(strings.Join(diagnosis.WhyLikely, "\n"), "legacy runtime telemetry") {
		t.Fatalf("expected legacy runtime note in diagnosis why_likely, got %v", diagnosis.WhyLikely)
	}
	if !strings.Contains(strings.Join(diagnosis.NextSteps, "\n"), "Refresh the worker fleet image") {
		t.Fatalf("expected fleet refresh note in diagnosis next_steps, got %v", diagnosis.NextSteps)
	}
}

func TestBuildCoverageSnapshotIncludesRuntimeStates(t *testing.T) {
	summaries := []WorkerSummaryResponse{
		{Worker: model.Worker{ID: "w1", ProfileName: "low-volume"}, Workload: workload.Config{LoadMode: "synthetic"}, RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusHealthy},
		{Worker: model.Worker{ID: "w2", ProfileName: "gharchive-replay"}, Workload: workload.Config{LoadMode: "replay", ReplayDataset: "gharchive"}, RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusLegacy},
		{Worker: model.Worker{ID: "w3", ProfileName: "high-volume"}, Workload: workload.Config{LoadMode: "synthetic"}, RuntimeSnapshotStatus: reporting.RuntimeSnapshotStatusLegacy},
	}

	coverage := buildCoverageSnapshot(summaries)
	if len(coverage.RuntimeStates) != 2 {
		t.Fatalf("runtime state count=%d want 2", len(coverage.RuntimeStates))
	}
	if coverage.RuntimeStates[0].Label != reporting.RuntimeSnapshotStatusLegacy || coverage.RuntimeStates[0].Count != 2 {
		t.Fatalf("top runtime state=%+v want legacy x2", coverage.RuntimeStates[0])
	}
}

func TestRelatedDiagnosisClustersPrefersWorkerCluster(t *testing.T) {
	diagnosis := diagnosisSnapshot{
		Clusters: []diagnosisCluster{
			{
				Key:               "sync|litestream_sync_socket_refused|Litestream sync/control socket",
				Signature:         "litestream_sync_socket_refused",
				ProbableSubsystem: "Litestream sync/control socket",
				Workers: []diagnosisWorkerRef{
					{ID: "worker-main-burst-vol", Name: "worker-main-burst-vol"},
					{ID: "worker-main-high-vol", Name: "worker-main-high-vol"},
				},
			},
			{
				Key:               "restore|replica_s3_timeout|Replication or restore path",
				Signature:         "replica_s3_timeout",
				ProbableSubsystem: "Replication or restore path",
				Workers: []diagnosisWorkerRef{
					{ID: "worker-main-taxi-replay", Name: "worker-main-taxi-replay"},
				},
			},
		},
	}

	clusters := relatedDiagnosisClusters(diagnosis, "worker-main-burst-vol", "litestream_sync_socket_refused", "Litestream sync/control socket")
	if len(clusters) == 0 {
		t.Fatal("expected related clusters")
	}
	if clusters[0].Signature != "litestream_sync_socket_refused" {
		t.Fatalf("first related cluster=%q", clusters[0].Signature)
	}
}

func TestBuildTriageCommandsUseBasicAuth(t *testing.T) {
	commands := buildTriageCommands(model.Worker{ID: "worker-main-burst-vol", AppName: "litestream-soak"}, false)
	text := strings.Join(commands, "\n")

	if !strings.Contains(text, "SOAK_BASIC_AUTH_USERNAME") {
		t.Fatalf("triage commands missing basic auth vars: %s", text)
	}
	if !strings.Contains(text, "/api/diagnosis") {
		t.Fatalf("triage commands missing diagnosis endpoint: %s", text)
	}
}

func TestBuildPromptIncludesFleetDiagnosisAndAuthGuidance(t *testing.T) {
	bundle := &IncidentBundle{
		GeneratedAt: timeMustParse("2026-04-11T21:00:00Z"),
		Worker: model.Worker{
			ID:     "worker-main-burst-vol",
			Status: model.WorkerDegraded,
		},
		Workload: workload.Config{
			LoadMode: "synthetic",
			Pattern:  "burst",
		},
		FailureStage:      "sync",
		FailureSignature:  "litestream_sync_socket_refused",
		ProbableSubsystem: "Litestream sync/control socket",
		Guide: incidentGuide{
			Summary:           "The latest verification is failing during sync.",
			ProbableSubsystem: "Litestream sync/control socket",
			WhyLikely:         []string{"The latest verification is still failing."},
			NextSteps:         []string{"Inspect the worker logs."},
		},
		Diagnosis: diagnosisSnapshot{
			Headline:          "2 active failure clusters across 4 workers",
			Summary:           "The dominant live cluster is litestream_sync_socket_refused during sync.",
			ProbableSubsystem: "Litestream sync/control socket",
			Confidence:        "high",
			AffectedWorkers:   4,
			DominantStage:     "sync",
			DominantSignature: "litestream_sync_socket_refused",
			WhyLikely:         []string{"3 workers share the dominant signature."},
			NextSteps:         []string{"Open a representative worker first."},
		},
		RelatedClusters: []diagnosisCluster{
			{
				Key:               "sync|litestream_sync_socket_refused|Litestream sync/control socket",
				Signature:         "litestream_sync_socket_refused",
				ProbableSubsystem: "Litestream sync/control socket",
			},
		},
		ReportedRuntime: &reporting.RuntimePayload{
			SnapshotCollectedAt:       timeMustParse("2026-04-11T21:00:10Z"),
			DBStatus:                  "unknown",
			LitestreamSnapshotHealthy: false,
			LitestreamSnapshotError:   "read txid: dial unix /data/litestream.sock: connect: connection refused",
		},
		TriageCommands: []string{
			`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/workers/worker-main-burst-vol/incident | jq .`,
			`curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" https://litestream-soak-ctl.fly.dev/api/diagnosis | jq .`,
		},
	}

	prompt := buildPrompt(bundle, promptModeLitestream)
	for _, want := range []string{
		"<fleet_diagnosis>",
		"<related_clusters>",
		"<control_plane_access>",
		"shared across the fleet or isolated",
		"<reported_runtime>",
		"litestream_snapshot_healthy",
		"SOAK_BASIC_AUTH_USERNAME",
		"/api/diagnosis",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q", want)
		}
	}
}

func timeMustParse(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return t
}
