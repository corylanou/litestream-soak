package model

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestRunIdentitySurvivesReopenAndReplacement(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "evidence.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{ID: "worker", Name: "worker", Source: "main", Status: WorkerRunning, ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	run := reporting.WorkerIdentity{WorkerID: worker.ID, DeploymentID: 3, Source: "main", GitSHA: "soak", LitestreamSHA: "candidate", WorkloadSHA: "generator", WorkloadID: "workload", ProfileConfig: "{}", ProfileHash: "44136fa355b3678a", ValidatorID: "soak-verifier:soak", RunID: "old-run", MachineID: "old-machine"}
	if err := db.ExpectWorkerRun(run); err != nil {
		t.Fatal(err)
	}
	attributed, quarantined, err := db.ReportAttribution(run)
	if err != nil || !attributed || quarantined {
		t.Fatalf("attribution = %v, %v, %v", attributed, quarantined, err)
	}
	v := Verification{WorkerID: worker.ID, Run: run, Attributed: attributed, StartedAt: time.Now().UTC(), Status: "failed", CheckType: "integrity", ErrorMessage: "preserved"}
	if err := db.RecordVerification(&v); err != nil {
		t.Fatal(err)
	}
	legacy := Verification{WorkerID: worker.ID, StartedAt: time.Now().UTC(), Status: "passed", Passed: true, CheckType: "integrity"}
	if err := db.RecordVerification(&legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	history, err := db.ListVerifications(worker.ID, 10)
	if err != nil || len(history) != 2 {
		t.Fatalf("history = %v, %v", history, err)
	}
	if history[0].Attributed || history[0].Run.RunID != "" {
		t.Fatalf("legacy record gained attribution: %+v", history[0])
	}
	if history[1].Run != run || !history[1].Attributed {
		t.Fatalf("identity changed: %+v", history[1])
	}
	replacement := run
	replacement.RunID = "new-run"
	replacement.MachineID = "new-machine"
	replacement.DeploymentID++
	if err := db.ExpectWorkerRun(replacement); err != nil {
		t.Fatal(err)
	}
	attributed, quarantined, err = db.ReportAttribution(run)
	if err != nil || attributed || !quarantined {
		t.Fatalf("stale attribution = %v, %v, %v", attributed, quarantined, err)
	}
	deployment := Deployment{WorkloadSHA: run.WorkloadSHA, ID: run.DeploymentID, Source: run.Source, GitSHA: run.GitSHA, LitestreamSHA: run.LitestreamSHA}
	worker.FlyMachineID = replacement.MachineID
	if !history[1].MatchesDeployment(worker, deployment) {
		t.Fatal("historical attribution depends on current machine")
	}
	if history[0].MatchesDeployment(worker, deployment) {
		t.Fatal("legacy record qualifies")
	}
	failed, err := db.GetLatestFailedVerification(worker.ID)
	if err != nil || failed == nil || failed.Run != run {
		t.Fatalf("failed evidence = %v, %v", failed, err)
	}
	recent, err := db.ListRecentFailedVerifications(10)
	if err != nil || len(recent) != 1 || recent[0].Run != run {
		t.Fatalf("recent evidence = %v, %v", recent, err)
	}
}

func TestReportAttributionLegacyAndDatabaseErrors(t *testing.T) {
	t.Parallel()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	attributed, quarantined, err := db.ReportAttribution(reporting.WorkerIdentity{WorkerID: "unknown"})
	if err != nil || attributed || quarantined {
		t.Fatalf("legacy attribution = %v, %v, %v", attributed, quarantined, err)
	}
	if _, err := db.exec(`INSERT INTO expected_worker_runs (worker_id,identity_json) VALUES ('bad','invalid')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExpectedWorkerRun("bad"); err == nil {
		t.Fatal("invalid stored identity accepted")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpectWorkerRun(reporting.WorkerIdentity{WorkerID: "closed"}); err == nil {
		t.Fatal("closed database accepted registration")
	}
	if _, _, err := db.ReportAttribution(reporting.WorkerIdentity{WorkerID: "closed"}); err == nil {
		t.Fatal("closed database accepted attribution")
	}
}

func TestDeploymentGeneratorProvenanceIsImmutable(t *testing.T) {
	t.Parallel()
	db, err := Open(filepath.Join(t.TempDir(), "deployments.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first := Deployment{Source: "main", GitSHA: "soak", LitestreamSHA: "candidate", WorkloadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ImageRef: "image", Status: "ready"}
	if err := db.UpsertReadyDeployment(&first); err != nil {
		t.Fatal(err)
	}
	before, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.WorkloadSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := db.UpsertReadyDeployment(&second); err != nil {
		t.Fatal(err)
	}
	after, err := db.GetLatestDeployment("main")
	if err != nil {
		t.Fatal(err)
	}
	if before.ID == after.ID || before.WorkloadSHA != first.WorkloadSHA || after.WorkloadSHA != second.WorkloadSHA {
		t.Fatalf("generator provenance overwritten: before=%+v after=%+v", before, after)
	}
	if err := db.UpsertReadyDeployment(&second); err != nil {
		t.Fatal(err)
	}
	deployments, err := db.ListDeployments("main", 10)
	if err != nil || len(deployments) != 2 {
		t.Fatalf("idempotent provenance = %v, %v", deployments, err)
	}
}

func TestMissingTrustedGeneratorCannotEarnDeploymentCredit(t *testing.T) {
	t.Parallel()
	db, err := Open(filepath.Join(t.TempDir(), "unknown-build.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expected := reporting.WorkerIdentity{WorkerID: "worker", DeploymentID: 1, GitSHA: "soak", LitestreamSHA: "candidate", RunID: "run", MachineID: "machine", WorkloadID: "workload", ProfileConfig: "{}", ProfileHash: "44136fa355b3678a", ValidatorID: "soak-verifier:soak"}
	if err := db.ExpectWorkerRun(expected); err != nil {
		t.Fatal(err)
	}
	report := expected
	report.WorkloadSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	attributed, quarantined, err := db.ReportAttribution(report)
	if err != nil || attributed || quarantined {
		t.Fatalf("unknown trusted generator: attributed=%v quarantined=%v err=%v", attributed, quarantined, err)
	}
}
