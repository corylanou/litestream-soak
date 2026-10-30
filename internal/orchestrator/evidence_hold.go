package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
)

const (
	eventEvidenceHoldApplied         = "evidence_hold_applied"
	eventEvidenceHoldReleased        = "evidence_hold_released"
	eventEvidenceHoldRetained        = "evidence_hold_retained"
	volumeGCEventSkippedEvidenceHold = "volume_gc_skipped_evidence_hold"
)

var errWorkerEvidenceHeld = errors.New("worker is under an evidence hold")

type EvidenceHoldResponse struct {
	WorkerID           string             `json:"worker_id"`
	Name               string             `json:"name"`
	Source             string             `json:"source"`
	Status             model.WorkerStatus `json:"status"`
	Held               bool               `json:"held"`
	AlreadyHeld        bool               `json:"already_held,omitempty"`
	Released           bool               `json:"released,omitempty"`
	Destroyed          bool               `json:"destroyed,omitempty"`
	DestroyError       string             `json:"destroy_error,omitempty"`
	EvidenceHoldAt     *time.Time         `json:"evidence_hold_at,omitempty"`
	EvidenceHoldReason string             `json:"evidence_hold_reason,omitempty"`
	FlyMachineID       string             `json:"fly_machine_id,omitempty"`
	FlyVolumeID        string             `json:"fly_volume_id,omitempty"`
	ReplicaPrefix      string             `json:"replica_prefix,omitempty"`
}

type RCGateHeldWorker struct {
	Source             string     `json:"source"`
	WorkerID           string     `json:"worker_id"`
	Name               string     `json:"name"`
	Profile            string     `json:"profile"`
	FlyMachineID       string     `json:"fly_machine_id,omitempty"`
	FlyVolumeID        string     `json:"fly_volume_id,omitempty"`
	ReplicaPath        string     `json:"replica_path"`
	EvidenceHoldAt     *time.Time `json:"evidence_hold_at,omitempty"`
	EvidenceHoldReason string     `json:"evidence_hold_reason,omitempty"`
}

func (m *Manager) workerReplicaURL(worker model.Worker) string {
	bucket := strings.TrimSpace(m.replica.Bucket)
	if bucket == "" {
		return ""
	}
	return fmt.Sprintf("s3://%s/%s/", bucket, strings.Trim(workerReplicaPath(worker), "/"))
}

func (m *Manager) evidenceHoldResponse(worker model.Worker) EvidenceHoldResponse {
	return EvidenceHoldResponse{
		WorkerID:           worker.ID,
		Name:               worker.Name,
		Source:             worker.Source,
		Status:             worker.Status,
		Held:               worker.EvidenceHeld(),
		EvidenceHoldAt:     worker.EvidenceHoldAt,
		EvidenceHoldReason: worker.EvidenceHoldReason,
		FlyMachineID:       worker.FlyMachineID,
		FlyVolumeID:        worker.FlyVolumeID,
		ReplicaPrefix:      m.workerReplicaURL(worker),
	}
}

func (m *Manager) HoldWorkerEvidence(ctx context.Context, workerID, reason string) (EvidenceHoldResponse, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return EvidenceHoldResponse{}, fmt.Errorf("evidence hold reason is required")
	}
	unlock, err := m.lockWorker(ctx, workerID)
	if err != nil {
		return EvidenceHoldResponse{}, err
	}
	defer unlock()

	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return EvidenceHoldResponse{}, fmt.Errorf("get worker: %w", err)
	}
	if worker.EvidenceHeld() {
		response := m.evidenceHoldResponse(*worker)
		response.AlreadyHeld = true
		return response, nil
	}
	if err := m.db.SetWorkerEvidenceHold(workerID, reason); err != nil {
		return EvidenceHoldResponse{}, fmt.Errorf("set evidence hold: %w", err)
	}
	worker, err = m.db.GetWorker(workerID)
	if err != nil {
		return EvidenceHoldResponse{}, fmt.Errorf("reload worker: %w", err)
	}
	response := m.evidenceHoldResponse(*worker)
	_ = m.db.RecordEvent(workerID, eventEvidenceHoldApplied, fmt.Sprintf("Evidence hold applied to %s; machine %s, volume %s and replica prefix %s are retained until released", worker.Name, valueOrUnknown(worker.FlyMachineID), valueOrUnknown(worker.FlyVolumeID), valueOrUnknown(response.ReplicaPrefix)), reason)
	slog.Info("Evidence hold applied", "worker_id", workerID, "name", worker.Name, "source", worker.Source, "reason", reason)
	return response, nil
}

func (m *Manager) ReleaseWorkerEvidenceHold(ctx context.Context, workerID string, destroy bool) (EvidenceHoldResponse, error) {
	unlock, err := m.lockWorker(ctx, workerID)
	if err != nil {
		return EvidenceHoldResponse{}, err
	}
	defer unlock()

	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return EvidenceHoldResponse{}, fmt.Errorf("get worker: %w", err)
	}
	previousReason := worker.EvidenceHoldReason
	released := worker.EvidenceHeld()
	if released {
		if err := m.db.ClearWorkerEvidenceHold(workerID); err != nil {
			return EvidenceHoldResponse{}, fmt.Errorf("clear evidence hold: %w", err)
		}
		_ = m.db.RecordEvent(workerID, eventEvidenceHoldReleased, fmt.Sprintf("Evidence hold released for %s; normal cleanup paths apply again", worker.Name), previousReason)
		slog.Info("Evidence hold released", "worker_id", workerID, "name", worker.Name, "source", worker.Source, "destroy", destroy)
	}
	response := m.evidenceHoldResponse(*worker)
	response.Held = false
	response.EvidenceHoldAt = nil
	response.EvidenceHoldReason = ""
	response.Released = released
	if destroy {
		if err := m.destroyWorker(ctx, workerID); err != nil {
			response.DestroyError = err.Error()
		} else {
			response.Destroyed = true
			response.Status = model.WorkerStopped
		}
	}
	return response, nil
}

func (m *Manager) retainHeldWorker(ctx context.Context, worker model.Worker) error {
	if worker.FlyMachineID != "" && m.fly != nil {
		if err := m.flyClientForWorker(worker).StopMachine(ctx, worker.FlyMachineID); err != nil && !flyapi.IsNotFound(err) {
			slog.Warn("Failed to stop held worker machine", "worker_id", worker.ID, "machine_id", worker.FlyMachineID, "error", err)
		}
	}
	if worker.Status != model.WorkerStopped {
		if err := m.db.UpdateWorkerStatus(worker.ID, model.WorkerStopped, ""); err != nil {
			return fmt.Errorf("mark held worker stopped: %w", err)
		}
		m.observeWorkerByID(worker.ID)
	}
	message := fmt.Sprintf("Evidence hold retained machine %s, volume %s and replica prefix %s for %s; destroy skipped", valueOrUnknown(worker.FlyMachineID), valueOrUnknown(worker.FlyVolumeID), valueOrUnknown(m.workerReplicaURL(worker)), worker.Name)
	if err := m.db.RecordEvent(worker.ID, eventEvidenceHoldRetained, message, worker.EvidenceHoldReason); err != nil {
		slog.Warn("Failed to record evidence hold retention", "worker_id", worker.ID, "error", err)
	}
	slog.Info("Evidence hold retained worker resources", "worker_id", worker.ID, "name", worker.Name, "machine_id", worker.FlyMachineID, "volume_id", worker.FlyVolumeID)
	return fmt.Errorf("%w: %s", errWorkerEvidenceHeld, worker.Name)
}

func (m *Manager) lockVolumeOwners(ctx context.Context, volumeID string) (*model.Worker, func(), error) {
	noop := func() {}
	if m.db == nil || strings.TrimSpace(volumeID) == "" {
		return nil, noop, nil
	}
	owners, err := m.db.ListWorkersReferencingVolume(volumeID)
	if err != nil {
		return nil, noop, fmt.Errorf("list workers referencing volume %s: %w", volumeID, err)
	}
	unlocks := make([]func(), 0, len(owners))
	unlockAll := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
	var held *model.Worker
	for _, owner := range owners {
		unlock, err := m.lockWorker(ctx, owner.ID)
		if err != nil {
			unlockAll()
			return nil, noop, err
		}
		unlocks = append(unlocks, unlock)
		worker, err := m.db.GetWorker(owner.ID)
		if err != nil {
			unlockAll()
			return nil, noop, fmt.Errorf("reload worker %s: %w", owner.ID, err)
		}
		if held == nil && worker.EvidenceHeld() {
			held = worker
		}
	}
	return held, unlockAll, nil
}

func (m *Manager) skipHeldVolumeGC(volumeID, volumeName string, held model.Worker, attempt *model.VolumeGCAttempt, now time.Time) {
	if attempt != nil {
		if err := m.removeVolumeGCAttempt(volumeID); err != nil {
			slog.Warn("Failed to clear persisted volume GC attempt for held volume", "volume_id", volumeID, "worker_id", held.ID, "error", err)
		}
	}
	message := fmt.Sprintf("Skipped volume GC for %s (%s): worker %s is under an evidence hold", valueOrUnknown(volumeName), volumeID, held.Name)
	details := fmt.Sprintf("worker_id=%s evidence_hold_at=%s reason=%s", held.ID, formatTime(held.EvidenceHoldAt), held.EvidenceHoldReason)
	m.recordVolumeGCEvent(volumeGCEventSkippedEvidenceHold, message, details, now, volumeGCOperatorEventWindow)
	slog.Info("Skipping volume GC for held worker volume", "volume_id", volumeID, "volume_name", volumeName, "worker_id", held.ID, "worker_name", held.Name)
}

func rcGateHeldWorkers(db *model.DB, gate model.RCGate) ([]RCGateHeldWorker, error) {
	var held []RCGateHeldWorker
	for _, source := range gate.Sources() {
		workers, err := db.ListWorkersFiltered("", source)
		if err != nil {
			return nil, fmt.Errorf("list %s workers: %w", source, err)
		}
		for _, worker := range workers {
			if !worker.EvidenceHeld() || !rcGateOwnsWorker(gate, worker) {
				continue
			}
			held = append(held, RCGateHeldWorker{
				Source:             source,
				WorkerID:           worker.ID,
				Name:               worker.Name,
				Profile:            worker.ProfileName,
				FlyMachineID:       worker.FlyMachineID,
				FlyVolumeID:        worker.FlyVolumeID,
				ReplicaPath:        workerReplicaPath(worker),
				EvidenceHoldAt:     worker.EvidenceHoldAt,
				EvidenceHoldReason: worker.EvidenceHoldReason,
			})
		}
	}
	return held, nil
}
