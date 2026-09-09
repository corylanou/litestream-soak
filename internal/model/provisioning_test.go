package model

import "testing"

func TestProvisioningCompletionIsAtomicWithEvidence(t *testing.T) {
	db := seriesTestDB(t)
	worker := Worker{ID: "pending", Name: "pending", Status: WorkerPending, Source: "main", GitSHA: "sha", LitestreamSHA: "ls", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	a, err := db.BeginProvisioning(ProvisioningAttempt{ID: "attempt", WorkerID: worker.ID, ImageRef: "image", GitSHA: "sha", LitestreamSHA: "ls", VolumeName: "owned", VolumeSizeGB: 10})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := db.AdvanceProvisioning(*a, "machine_ready", "volume", "machine")
	if err != nil || !changed {
		t.Fatal(err)
	}
	a.Phase = "machine_ready"
	a.VolumeID = "volume"
	a.MachineID = "machine"
	if _, err := db.writer.Exec(`CREATE TRIGGER reject_completion BEFORE INSERT ON events WHEN NEW.event_type='worker_started' BEGIN SELECT RAISE(FAIL,'event storage unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	events := []Event{{EventType: "worker_provisioning_recovered", Message: "recovered interrupted attempt", Details: "{}"}, {EventType: "worker_started", Message: "started", Details: "{}"}}
	if err := db.CompleteProvisioning(*a, events); err == nil {
		t.Fatal("completion ignored evidence failure")
	}
	stored, err := db.GetWorker(worker.ID)
	if err != nil || stored.Status != WorkerPending {
		t.Fatalf("false completion: %+v %v", stored, err)
	}
	active, err := db.ActiveProvisioning(worker.ID)
	if err != nil || active == nil || active.Phase != "machine_ready" {
		t.Fatalf("lost active attempt: %+v %v", active, err)
	}
	retained, err := db.ListEvidenceEvents("main")
	if err != nil || len(retained) != 0 {
		t.Fatalf("partial evidence committed: %+v %v", retained, err)
	}
	if _, err := db.writer.Exec(`DROP TRIGGER reject_completion`); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteProvisioning(*a, events); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteProvisioning(*a, events); err == nil {
		t.Fatal("stale completion claim succeeded")
	}
	retained, err = db.ListEvidenceEvents("main")
	if err != nil || len(retained) != 2 {
		t.Fatalf("lost/duplicated completion events: %+v %v", retained, err)
	}
	if _, err := db.BeginProvisioning(ProvisioningAttempt{ID: "stale-controller", WorkerID: worker.ID, ImageRef: "image", GitSHA: "sha", LitestreamSHA: "ls", VolumeName: "other", VolumeSizeGB: 10}); err == nil {
		t.Fatal("stale controller started duplicate provisioning after worker became running")
	}
}
