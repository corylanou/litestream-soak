package model

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func (d *DB) LockWorkerReports(workerID string) func() {
	value, _ := d.workerReports.LoadOrStore(workerID, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func (d *DB) ExpectWorkerRun(identity reporting.WorkerIdentity) error {
	unlock := d.LockWorkerReports(identity.WorkerID)
	defer unlock()
	body, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	_, err = d.exec(`INSERT INTO expected_worker_runs (worker_id, identity_json) VALUES (?, ?) ON CONFLICT(worker_id) DO UPDATE SET identity_json = excluded.identity_json`, identity.WorkerID, string(body))
	return err
}

func (d *DB) ExpectedWorkerRun(workerID string) (*reporting.WorkerIdentity, error) {
	var body string
	err := d.queryRow(`SELECT identity_json FROM expected_worker_runs WHERE worker_id = ?`, workerID).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var identity reporting.WorkerIdentity
	if err := json.Unmarshal([]byte(body), &identity); err != nil {
		return nil, fmt.Errorf("decode expected worker run: %w", err)
	}
	return &identity, nil
}

func (d *DB) ReportAttribution(identity reporting.WorkerIdentity) (bool, bool, error) {
	expected, err := d.ExpectedWorkerRun(identity.WorkerID)
	if err != nil {
		return false, false, err
	}
	if expected == nil {
		worker, err := d.GetWorker(identity.WorkerID)
		if err == sql.ErrNoRows {
			return false, false, nil
		}
		if err != nil {
			return false, false, err
		}
		return false, worker.FlyMachineID != "" || worker.AppName != "", nil
	}
	profileDigest := sha256.Sum256([]byte(identity.ProfileConfig))
	matches := identity.RunID != "" && identity.RunID == expected.RunID &&
		identity.DeploymentID == expected.DeploymentID &&
		identity.GitSHA == expected.GitSHA &&
		identity.LitestreamSHA == expected.LitestreamSHA &&
		identity.ImageRef == expected.ImageRef &&
		identity.Source == expected.Source && identity.ProfileName == expected.ProfileName &&
		identity.WorkloadID != "" && identity.WorkloadID == expected.WorkloadID &&
		identity.ProfileConfig != "" && identity.ProfileConfig == expected.ProfileConfig &&
		identity.ProfileHash == fmt.Sprintf("%x", profileDigest[:8]) &&
		identity.ValidatorID == "soak-verifier:"+expected.GitSHA &&
		identity.MachineID != "" && identity.MachineID == expected.MachineID &&
		(expected.WorkloadSHA == "" || identity.WorkloadSHA == expected.WorkloadSHA)
	return matches && identity.WorkloadSHA != "" && identity.WorkloadSHA == expected.WorkloadSHA && identity.DeploymentID > 0 && identity.GitSHA != "" && identity.LitestreamSHA != "", !matches, nil
}

func (v Verification) MatchesDeployment(worker Worker, deployment Deployment) bool {
	return v.Attributed && v.Run.DeploymentID > 0 && v.Run.DeploymentID == deployment.ID &&
		v.Run.GitSHA == deployment.GitSHA && v.Run.LitestreamSHA == deployment.LitestreamSHA &&
		v.Run.WorkloadSHA == deployment.WorkloadSHA &&
		v.Run.Source == deployment.Source && v.Run.WorkerID == worker.ID &&
		v.Run.MachineID != "" &&
		v.Run.RunID != "" && v.Run.ProfileHash != "" && v.Run.ValidatorID != ""
}
