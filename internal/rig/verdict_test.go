package rig

import (
	"context"
	"errors"
	"testing"
)

func TestRecoveryVerdict(t *testing.T) {
	tests := []struct {
		name     string
		evidence RecoveryEvidence
		want     string
	}{
		{"valid exposed fixture", RecoveryEvidence{ValidRestores: 3, ExposedRestores: 3}, "scenario_success"},
		{"target failure fixture", RecoveryEvidence{TargetFailures: 3, ExposedRestores: 3}, "target_signature_observed"},
		{"unrelated failure without signature", RecoveryEvidence{ValidRestores: 1, OtherFailures: 1, ExposedRestores: 1}, "unrelated_failure"},
		{"self healing failure", RecoveryEvidence{ValidRestores: 3, TargetFailures: 1, ExposedRestores: 3}, "target_signature_observed"},
		{"zero exposure", RecoveryEvidence{ValidRestores: 3}, "inconclusive"},
		{"no valid restores", RecoveryEvidence{ExposedRestores: 3}, "inconclusive"},
		{"aborted", RecoveryEvidence{Aborted: true, ValidRestores: 1, ExposedRestores: 1}, "aborted"},
		{"both failure classes", RecoveryEvidence{TargetFailures: 1, OtherFailures: 1}, "unrelated_failure"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.evidence.Verdict(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMaintenanceExposure(t *testing.T) {
	for _, tt := range []struct {
		l0, retention, compaction, busy int
		want                            bool
	}{
		{1, 1, 1, 0, true}, {0, 1, 1, 0, false}, {1, 0, 1, 0, false}, {1, 1, 0, 0, false}, {1, -1, 1, 0, false}, {1, 0, 0, 1, false}, {0, 0, 0, 1, false},
	} {
		if got := MaintenanceExposed(tt.l0, tt.retention, tt.compaction); got != tt.want {
			t.Fatalf("%+v: got %v", tt, got)
		}
	}
}

func TestAttemptVerdict(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		err                       error
		cancelled, valid, exposed bool
		want                      string
	}{
		{"cancelled validation", context.DeadlineExceeded, true, false, true, "aborted"},
		{"operation timeout", context.DeadlineExceeded, false, false, true, "unrelated_failure"},
		{"failure coincides with cancellation", errors.New("unrelated failure"), true, false, true, "unrelated_failure"},
		{"target coincides with cancellation", errors.New("reopen ltx file at offset 0: file does not exist"), true, false, true, "target_signature_observed"},
		{"valid but unexposed", nil, false, true, false, "inconclusive"},
		{"valid and exposed", nil, false, true, true, "scenario_success"},
		{"invalid", nil, false, false, true, "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := AttemptVerdict(tt.err, tt.cancelled, tt.valid, tt.exposed); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}
