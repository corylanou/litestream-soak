package model

import (
	"database/sql"
	"fmt"
)

type ProvisioningAttempt struct {
	VolumeSizeGB  int    `json:"volume_size_gb"`
	ID            string `json:"attempt_id"`
	WorkerID      string `json:"worker_id"`
	ImageRef      string `json:"image_ref"`
	GitSHA        string `json:"git_sha"`
	LitestreamSHA string `json:"litestream_sha"`
	Phase         string `json:"phase"`
	VolumeName    string `json:"volume_name"`
	VolumeID      string `json:"volume_id"`
	MachineID     string `json:"machine_id"`
}

const provisioningSchema = `CREATE TABLE IF NOT EXISTS worker_provisioning (
 id TEXT PRIMARY KEY, worker_id TEXT NOT NULL, image_ref TEXT NOT NULL, git_sha TEXT NOT NULL, litestream_sha TEXT NOT NULL,
 phase TEXT NOT NULL, volume_size_gb INTEGER NOT NULL, volume_name TEXT NOT NULL, volume_id TEXT NOT NULL DEFAULT '', machine_id TEXT NOT NULL DEFAULT '', started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS worker_provisioning_active ON worker_provisioning(worker_id) WHERE phase!='complete';`

func (d *DB) ActiveProvisioning(workerID string) (*ProvisioningAttempt, error) {
	var a ProvisioningAttempt
	err := d.queryRow(`SELECT id,worker_id,image_ref,git_sha,litestream_sha,phase,volume_name,volume_id,machine_id,volume_size_gb FROM worker_provisioning WHERE worker_id=? AND phase!='complete'`, workerID).Scan(&a.ID, &a.WorkerID, &a.ImageRef, &a.GitSHA, &a.LitestreamSHA, &a.Phase, &a.VolumeName, &a.VolumeID, &a.MachineID, &a.VolumeSizeGB)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (d *DB) BeginProvisioning(a ProvisioningAttempt) (*ProvisioningAttempt, error) {
	if _, err := d.exec(`INSERT OR IGNORE INTO worker_provisioning(id,worker_id,image_ref,git_sha,litestream_sha,phase,volume_name,volume_size_gb) SELECT ?,?,?,?,?,'planned',?,? FROM workers WHERE id=? AND status='pending' AND git_sha=? AND litestream_sha=?`, a.ID, a.WorkerID, a.ImageRef, a.GitSHA, a.LitestreamSHA, a.VolumeName, a.VolumeSizeGB, a.WorkerID, a.GitSHA, a.LitestreamSHA); err != nil {
		return nil, err
	}
	current, err := d.ActiveProvisioning(a.WorkerID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.ImageRef != a.ImageRef || current.GitSHA != a.GitSHA || current.LitestreamSHA != a.LitestreamSHA {
		return nil, fmt.Errorf("another provisioning target is unresolved")
	}
	return current, nil
}

func (d *DB) AdvanceProvisioning(a ProvisioningAttempt, phase, volumeID, machineID string) (bool, error) {
	result, err := d.exec(`UPDATE worker_provisioning SET phase=?,volume_id=COALESCE(NULLIF(?,''),volume_id),machine_id=COALESCE(NULLIF(?,''),machine_id) WHERE id=? AND phase=?`, phase, volumeID, machineID, a.ID, a.Phase)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func (d *DB) CompleteProvisioning(a ProvisioningAttempt, events []Event) error {
	if a.Phase != "machine_ready" || a.MachineID == "" || a.VolumeID == "" {
		return fmt.Errorf("provisioning is not ready to complete")
	}
	unlock := d.LockWorkerReports(a.WorkerID)
	defer unlock()
	tx, err := d.writer.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.Exec(`UPDATE worker_provisioning SET phase='complete' WHERE id=? AND phase='machine_ready' AND volume_id=? AND machine_id=?`, a.ID, a.VolumeID, a.MachineID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("provisioning completion lost its state claim")
	}
	result, err = tx.Exec(`UPDATE workers SET fly_machine_id=?,fly_volume_id=?,status='running',error_message='',updated_at=datetime('now') WHERE id=? AND git_sha=? AND litestream_sha=? AND status IN ('pending','running','starting','probing')`, a.MachineID, a.VolumeID, a.WorkerID, a.GitSHA, a.LitestreamSHA)
	if err != nil {
		return err
	}
	n, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("worker changed before provisioning completion")
	}
	for _, event := range events {
		if _, err := tx.Exec(`INSERT INTO events(worker_id,event_type,message,details) VALUES(?,?,?,?)`, a.WorkerID, event.EventType, event.Message, event.Details); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) BindProvisioningMachine(a ProvisioningAttempt) error {
	unlock := d.LockWorkerReports(a.WorkerID)
	defer unlock()
	result, err := d.exec(`UPDATE workers SET fly_machine_id=?,fly_volume_id=?,updated_at=datetime('now') WHERE id=? AND git_sha=? AND litestream_sha=? AND status IN ('pending','running','starting','probing') AND EXISTS (SELECT 1 FROM worker_provisioning WHERE id=? AND worker_id=workers.id AND phase='machine_ready' AND machine_id=? AND volume_id=?)`, a.MachineID, a.VolumeID, a.WorkerID, a.GitSHA, a.LitestreamSHA, a.ID, a.MachineID, a.VolumeID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("provisioning binding lost its state claim")
	}
	return nil
}
