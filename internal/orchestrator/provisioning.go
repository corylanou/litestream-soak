package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	workerconfig "github.com/corylanou/litestream-soak/internal/worker"
	"github.com/corylanou/litestream-soak/internal/workload"
	"github.com/google/uuid"
)

func (m *Manager) beginWorkerProvisioning(worker model.Worker, image string, sizes ...int) (*model.ProvisioningAttempt, error) {
	size := resolveWorkerVolumeSize(worker, resolveWorkerWorkload(worker))
	if len(sizes) > 0 {
		size = sizes[0]
	}
	if size <= 0 {
		size = 10
	}
	id := uuid.NewString()
	attempt, err := m.db.BeginProvisioning(model.ProvisioningAttempt{VolumeSizeGB: size, ID: id, WorkerID: worker.ID, ImageRef: image, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, VolumeName: "soak_" + strings.ReplaceAll(id, "-", "")[:24]})
	if err != nil {
		return nil, err
	}
	if attempt.ID == id {
		if err := m.recordProvisioning(worker, *attempt, "worker_provisioning_started", "Provisioning intent recorded before resource creation"); err != nil {
			return nil, err
		}
	}
	return attempt, nil
}

func provisioningEvent(worker model.Worker, a model.ProvisioningAttempt, kind, message string) (model.Event, error) {
	body, err := json.Marshal(struct {
		reporting.WorkerIdentity
		AttemptID string `json:"attempt_id"`
		Phase     string `json:"phase"`
		VolumeID  string `json:"provisioning_volume_id"`
		MachineID string `json:"provisioning_machine_id"`
	}{WorkerIdentity: reporting.WorkerIdentity{WorkerID: worker.ID, Source: worker.Source, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, ProfileName: worker.ProfileName, Region: worker.Region, RunID: a.ID, MachineID: worker.FlyMachineID}, AttemptID: a.ID, Phase: a.Phase, VolumeID: a.VolumeID, MachineID: a.MachineID})
	if err != nil {
		return model.Event{}, err
	}
	return model.Event{WorkerID: worker.ID, EventType: kind, Message: message, Details: string(body)}, nil
}

func (m *Manager) recordProvisioning(worker model.Worker, a model.ProvisioningAttempt, kind, message string) error {
	event, err := provisioningEvent(worker, a, kind, message)
	if err != nil {
		return err
	}
	return m.db.RecordEvent(event.WorkerID, event.EventType, event.Message, event.Details)
}

func (m *Manager) advanceProvisioning(a model.ProvisioningAttempt, phase, volumeID, machineID string) error {
	advanced, err := m.db.AdvanceProvisioning(a, phase, volumeID, machineID)
	if err != nil {
		return err
	}
	if !advanced {
		return fmt.Errorf("provisioning state advanced concurrently")
	}
	return nil
}

func (m *Manager) provisioningUnavailable(worker model.Worker, a model.ProvisioningAttempt, message string, causes ...error) error {
	if len(causes) > 0 && causes[0] != nil {
		cause := causes[0]
		var apiErr *flyapi.APIError
		var netErr net.Error
		classification := "operation error"
		switch {
		case errors.Is(cause, context.Canceled):
			classification = "canceled"
		case errors.Is(cause, context.DeadlineExceeded):
			classification = "timeout"
		case errors.As(cause, &apiErr):
			classification = fmt.Sprintf("HTTP %d", apiErr.StatusCode)
		case errors.As(cause, &netErr) && netErr.Timeout():
			classification = "timeout"
		case errors.As(cause, &netErr):
			classification = "network error"
		}
		message += " (" + classification + ")"
	}
	if err := m.recordProvisioning(worker, a, "worker_provisioning_unavailable", message); err != nil {
		return err
	}
	return fmt.Errorf("provisioning unavailable: %s", message)
}

func (m *Manager) resumeWorkerProvisioning(ctx context.Context, worker model.Worker, a model.ProvisioningAttempt, cfg workload.Config, recovered bool) (*model.Worker, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.Phase == "planned" {
		claimed, err := m.db.AdvanceProvisioning(a, "volume_requested", "", "")
		if err != nil {
			return nil, err
		}
		if !claimed {
			return nil, fmt.Errorf("provisioning advanced concurrently")
		}
		a.Phase = "volume_requested"
		size := a.VolumeSizeGB
		if size == 0 {
			size = 10
		}
		volume, err := m.createWorkerVolume(ctx, worker, size)
		if err != nil {
			return nil, m.provisioningUnavailable(worker, a, "Volume creation outcome requires resource discovery", err)
		}
		saved, err := m.db.AdvanceProvisioning(a, "volume_ready", volume.ID, "")
		if err != nil {
			return nil, err
		}
		if !saved {
			return nil, fmt.Errorf("volume state advanced concurrently")
		}
		a.Phase = "volume_ready"
		a.VolumeID = volume.ID
	}
	if a.Phase == "volume_ready" {
		claimed, err := m.db.AdvanceProvisioning(a, "machine_requested", "", "")
		if err != nil {
			return nil, err
		}
		if !claimed {
			return nil, fmt.Errorf("provisioning advanced concurrently")
		}
		a.Phase = "machine_requested"
		worker.FlyVolumeID = a.VolumeID
		machine, err := m.createWorkerMachine(ctx, worker, a.ImageRef, a.VolumeID, cfg)
		if err != nil {
			return nil, m.provisioningUnavailable(worker, a, "Machine creation outcome requires resource discovery", err)
		}
		saved, err := m.db.AdvanceProvisioning(a, "machine_ready", "", machine.ID)
		if err != nil {
			return nil, err
		}
		if !saved {
			return nil, fmt.Errorf("machine state advanced concurrently")
		}
		a.Phase = "machine_ready"
		a.MachineID = machine.ID
		return m.finishWorkerProvisioning(worker, a, *machine, recovered)
	}
	return nil, m.provisioningUnavailable(worker, a, "Creation intent exists without a confirmed resource; refusing another create request")
}

func (m *Manager) finishWorkerProvisioning(worker model.Worker, a model.ProvisioningAttempt, machine flyapi.Machine, recovered bool) (*model.Worker, error) {
	worker.FlyMachineID = machine.ID
	worker.FlyVolumeID = a.VolumeID
	if machine.State != "started" {
		if err := m.db.BindProvisioningMachine(a); err != nil {
			return nil, err
		}
		return &worker, nil
	}
	var events []model.Event
	started, err := provisioningEvent(worker, a, "worker_started", fmt.Sprintf("Worker %s started (machine %s)", worker.Name, machine.ID))
	if err != nil {
		return nil, err
	}
	events = append(events, started)
	if recovered {
		event, err := provisioningEvent(worker, a, "worker_provisioning_recovered", "Recovered interrupted provisioning using confirmed owned resources")
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := m.db.CompleteProvisioning(a, events); err != nil {
		return nil, err
	}
	worker.Status = model.WorkerRunning
	m.observeWorkerByID(worker.ID)
	slog.Info("Worker created", "name", worker.Name, "machine_id", machine.ID, "volume_id", a.VolumeID, "profile", worker.ProfileName)
	return &worker, nil
}

func (m *Manager) recoverPendingWorker(ctx context.Context, workerID, image string) error {
	unlock, err := m.lockWorker(ctx, workerID)
	if err != nil {
		return err
	}
	defer unlock()
	worker, err := m.db.GetWorker(workerID)
	if err != nil {
		return err
	}
	a, err := m.db.ActiveProvisioning(workerID)
	if err != nil {
		return err
	}
	if worker.Status == model.WorkerStopped || worker.Status == model.WorkerDormant || worker.Status == model.WorkerFailed {
		return nil
	}
	if worker.Status != model.WorkerPending && a == nil {
		return nil
	}
	report := model.ProvisioningAttempt{WorkerID: workerID, Phase: "legacy_pending"}
	if a != nil {
		report = *a
	}
	client := m.flyClientForWorker(*worker)
	machines, err := client.ListMachines(ctx)
	if err != nil {
		return m.provisioningUnavailable(*worker, report, "Machine inventory could not be observed", err)
	}
	volumes, err := client.ListVolumes(ctx)
	if err != nil {
		return m.provisioningUnavailable(*worker, report, "Volume inventory could not be observed", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	recovered := a == nil || a.Phase != "machine_ready"
	if recovered {
		if err := m.recordProvisioning(*worker, report, "worker_provisioning_interrupted", "Pending provisioning requires reconciliation after an incomplete attempt"); err != nil {
			return err
		}
	}
	if a == nil {
		expected, err := m.db.ExpectedWorkerRun(workerID)
		if err != nil {
			return err
		}
		var owned []flyapi.Machine
		for _, machine := range machines {
			if machine.State == "destroyed" {
				continue
			}
			if machine.Name != worker.Name && machine.Config.Env["WORKER_ID"] != workerID {
				continue
			}
			if expected == nil || expected.RunID == "" || machine.Config.Env["SOAK_RUN_ID"] != expected.RunID || machine.CreatedAt.Before(worker.CreatedAt) || !provisioningMachineMatches(machine, *worker, image, expected.RunID) || !provisioningExpectedMachineMatches(machine, expected) {
				return m.provisioningUnavailable(*worker, report, "Existing machine ownership or target is ambiguous")
			}
			owned = append(owned, machine)
		}
		if len(owned) > 1 {
			return m.provisioningUnavailable(*worker, report, "Multiple matching machines require operator accounting")
		}
		if len(owned) == 1 {
			machine := owned[0]
			volume, ok := mountedProvisioningVolume(machine, volumes)
			if !ok || volume.Region != worker.Region || volume.SizeGB < max(10, resolveWorkerVolumeSize(*worker, resolveWorkerWorkload(*worker))) {
				return m.provisioningUnavailable(*worker, report, "Existing machine volume ownership is unconfirmed")
			}
			attempt, err := m.db.BeginProvisioning(model.ProvisioningAttempt{ID: expected.RunID, WorkerID: workerID, ImageRef: image, GitSHA: worker.GitSHA, LitestreamSHA: worker.LitestreamSHA, VolumeName: volume.Name, VolumeSizeGB: volume.SizeGB})
			if err != nil {
				return err
			}
			if err := m.advanceProvisioning(*attempt, "machine_ready", volume.ID, machine.ID); err != nil {
				return err
			}
			attempt.Phase = "machine_ready"
			attempt.VolumeID = volume.ID
			attempt.MachineID = machine.ID
			expected.MachineID = machine.ID
			expected.VolumeID = volume.ID
			if err := m.db.ExpectWorkerRun(*expected); err != nil {
				return err
			}
			_, err = m.finishWorkerProvisioning(*worker, *attempt, machine, true)
			return err
		}
		for _, volume := range volumes {
			if volume.ID == worker.FlyVolumeID || volume.Name == flyVolumeName(worker.Name) {
				return m.provisioningUnavailable(*worker, report, "Legacy volume may contain retained evidence; ownership needs operator accounting")
			}
		}
		if worker.FlyMachineID != "" {
			machine, err := client.GetMachine(ctx, worker.FlyMachineID)
			if err != nil && !flyapi.IsNotFound(err) {
				return m.provisioningUnavailable(*worker, report, "Previous machine state is unavailable", err)
			}
			if err == nil && machine.State != "destroyed" {
				return m.provisioningUnavailable(*worker, report, "Previous machine still exists")
			}
		}
		a, err = m.beginWorkerProvisioning(*worker, image)
		if err != nil {
			return err
		}
	}
	if a.GitSHA != worker.GitSHA || a.LitestreamSHA != worker.LitestreamSHA || a.ImageRef != image {
		return m.provisioningUnavailable(*worker, *a, "Unresolved attempt targets a different deployment")
	}
	var ownedVolumes []flyapi.Volume
	for _, volume := range volumes {
		if volume.Name == a.VolumeName {
			ownedVolumes = append(ownedVolumes, volume)
		}
	}
	if len(ownedVolumes) > 1 {
		return m.provisioningUnavailable(*worker, *a, "Multiple attempt volumes require operator accounting")
	}
	if len(ownedVolumes) == 1 {
		volume := ownedVolumes[0]
		if volume.SizeGB < a.VolumeSizeGB || volume.Region != worker.Region || volume.State == "destroyed" || (a.VolumeID != "" && a.VolumeID != volume.ID) {
			return m.provisioningUnavailable(*worker, *a, "Attempt volume state is inconsistent")
		}
		if a.Phase == "volume_requested" {
			if err := m.advanceProvisioning(*a, "volume_ready", volume.ID, ""); err != nil {
				return err
			}
			a.Phase = "volume_ready"
			a.VolumeID = volume.ID
		}
	}
	var ownedMachines []flyapi.Machine
	for _, machine := range machines {
		if machine.State == "destroyed" {
			continue
		}
		if machine.Config.Env["SOAK_RUN_ID"] != a.ID {
			continue
		}
		if !provisioningMachineMatches(machine, *worker, a.ImageRef, a.ID) {
			return m.provisioningUnavailable(*worker, *a, "Attempt machine does not match intended configuration")
		}
		ownedMachines = append(ownedMachines, machine)
	}
	if len(ownedMachines) > 1 {
		return m.provisioningUnavailable(*worker, *a, "Multiple attempt machines require operator accounting")
	}
	if len(ownedMachines) == 1 {
		machine := ownedMachines[0]
		volume, ok := mountedProvisioningVolume(machine, volumes)
		if !ok || volume.Name != a.VolumeName || (a.VolumeID != "" && a.VolumeID != volume.ID) {
			return m.provisioningUnavailable(*worker, *a, "Attempt machine mount is inconsistent")
		}
		expected, err := m.db.ExpectedWorkerRun(workerID)
		if err != nil {
			return err
		}
		if expected == nil || expected.RunID != a.ID || !provisioningExpectedMachineMatches(machine, expected) {
			return m.provisioningUnavailable(*worker, *a, "Expected immutable run identity is unavailable")
		}
		expected.MachineID = machine.ID
		expected.VolumeID = volume.ID
		if err := m.db.ExpectWorkerRun(*expected); err != nil {
			return err
		}
		if err := m.advanceProvisioning(*a, "machine_ready", volume.ID, machine.ID); err != nil {
			return err
		}
		a.Phase = "machine_ready"
		a.VolumeID = volume.ID
		a.MachineID = machine.ID
		_, err = m.finishWorkerProvisioning(*worker, *a, machine, recovered)
		return err
	}
	if a.VolumeID != "" && len(ownedVolumes) == 0 {
		return m.provisioningUnavailable(*worker, *a, "Persisted attempt volume is missing")
	}
	if len(ownedVolumes) == 1 && ownedVolumes[0].AttachedMachineID != "" {
		return m.provisioningUnavailable(*worker, *a, "Attempt volume is attached to an unconfirmed machine")
	}
	_, err = m.resumeWorkerProvisioning(ctx, *worker, *a, resolveWorkerWorkload(*worker), true)
	return err
}

func provisioningMachineMatches(machine flyapi.Machine, worker model.Worker, image, run string) bool {
	env := machine.Config.Env
	return machine.Name == worker.Name && machine.Region == worker.Region && machine.Config.Image == image && env["WORKER_ID"] == worker.ID && env["GIT_SHA"] == worker.GitSHA && env["LITESTREAM_SHA"] == worker.LitestreamSHA && env["SOURCE"] == worker.Source && env["SOAK_RUN_ID"] == run
}

func mountedProvisioningVolume(machine flyapi.Machine, volumes []flyapi.Volume) (flyapi.Volume, bool) {
	if len(machine.Config.Mounts) != 1 || machine.Config.Mounts[0].Path != "/data" {
		return flyapi.Volume{}, false
	}
	for _, volume := range volumes {
		if volume.ID == machine.Config.Mounts[0].Volume && volume.State != "destroyed" && (volume.AttachedMachineID == "" || volume.AttachedMachineID == machine.ID) {
			return volume, true
		}
	}
	return flyapi.Volume{}, false
}

func provisioningExpectedMachineMatches(machine flyapi.Machine, expected *reporting.WorkerIdentity) bool {
	if expected == nil || expected.WorkloadID == "" || machine.Config.Env["SOAK_WORKLOAD_ID"] != expected.WorkloadID {
		return false
	}
	cfg, err := workerconfig.WorkloadFromEnvironment(machine.Config.Env)
	if err != nil {
		return false
	}
	profile := cfg.JSON()
	digest := sha256.Sum256([]byte(profile))
	if profile != expected.ProfileConfig || fmt.Sprintf("%x", digest) != expected.WorkloadID {
		return false
	}
	if len(machine.Config.Mounts) != 1 || machine.Config.Env["SOAK_VOLUME_ID"] != machine.Config.Mounts[0].Volume {
		return false
	}
	return expected.VolumeID == "" || expected.VolumeID == machine.Config.Mounts[0].Volume
}
