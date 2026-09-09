package orchestrator

import "time"

func EvaluateRunEvidence(e WorkerRunEvidence, start, end time.Time) WorkerRunEvidence {
	evaluateRunEligibility(&e, start, &end, time.Hour, 24*time.Hour)
	return e
}
