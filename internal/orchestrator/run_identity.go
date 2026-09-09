package orchestrator

import "github.com/corylanou/litestream-soak/internal/model"

func deploymentVerifications(worker model.Worker, deployment model.Deployment, verifications []model.Verification) []model.Verification {
	filtered := make([]model.Verification, 0, len(verifications))
	for _, verification := range verifications {
		if verification.MatchesDeployment(worker, deployment) {
			filtered = append(filtered, verification)
		}
	}
	return filtered
}

func currentDeploymentVerifications(db *model.DB, worker model.Worker, deployment model.Deployment, verifications []model.Verification) ([]model.Verification, error) {
	expected, err := db.ExpectedWorkerRun(worker.ID)
	if err != nil {
		return nil, err
	}
	filtered := deploymentVerifications(worker, deployment, verifications)
	current := filtered[:0]
	for _, verification := range filtered {
		if verification.Run.MachineID == worker.FlyMachineID && (expected == nil || verification.Run.RunID == expected.RunID) {
			current = append(current, verification)
		}
	}
	return current, nil
}
