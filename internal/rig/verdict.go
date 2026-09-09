package rig

import (
	"context"
	"errors"
	"strings"
)

type RecoveryEvidence struct {
	ValidRestores   int  `json:"valid_restores"`
	ExposedRestores int  `json:"exposed_restores"`
	TargetFailures  int  `json:"target_failures"`
	OtherFailures   int  `json:"other_failures"`
	Aborted         bool `json:"aborted"`
}

func (e RecoveryEvidence) Verdict() string {
	switch {
	case e.OtherFailures > 0:
		return "unrelated_failure"
	case e.TargetFailures > 0:
		return "target_signature_observed"
	case e.Aborted:
		return "aborted"
	case e.ValidRestores <= 0 || e.ExposedRestores <= 0:
		return "inconclusive"
	default:
		return "scenario_success"
	}
}

func MaintenanceExposed(observedL0Opens, deletedL0Files, compactions int) bool {
	return observedL0Opens > 0 && deletedL0Files > 0 && compactions > 0
}

func AttemptVerdict(err error, cancelled, valid, exposed bool) string {
	if err != nil {
		if cancelled && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return "aborted"
		}
		if strings.Contains(err.Error(), "reopen ltx file") && strings.Contains(err.Error(), "file does not exist") {
			return "target_signature_observed"
		}
		return "unrelated_failure"
	}
	if !valid || !exposed {
		return "inconclusive"
	}
	return "scenario_success"
}
