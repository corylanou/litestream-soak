package orchestrator

import (
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestDetectDormancyCandidate(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-25*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	candidate, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 3)
	if !ok {
		t.Fatal("expected dormancy candidate")
	}
	if candidate.Signature != "litestream_sync_socket_refused" {
		t.Fatalf("signature=%q want %q", candidate.Signature, "litestream_sync_socket_refused")
	}
	if candidate.Count != 3 {
		t.Fatalf("count=%d want 3", candidate.Count)
	}
	if !candidate.Since.Equal(now.Add(-25 * time.Hour)) {
		t.Fatalf("since=%s want %s", candidate.Since, now.Add(-25*time.Hour))
	}
}

func TestDetectDormancyCandidateRequiresConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	passCompletedAt := now.Add(-8 * time.Hour)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		{
			StartedAt:    passCompletedAt.Add(-10 * time.Second),
			CompletedAt:  &passCompletedAt,
			Status:       "completed",
			CheckType:    "integrity",
			Passed:       true,
			ErrorMessage: "",
		},
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when a pass interrupts the run")
	}
}

func TestDetectDormancyCandidateRequiresSameSignature(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	verifications := []model.Verification{
		failedVerificationAt(now.Add(-30*time.Minute), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
		failedVerificationAt(now.Add(-12*time.Hour), `wait for sync: context deadline exceeded`),
		failedVerificationAt(now.Add(-30*time.Hour), `wait for sync: sync request: Post "http://localhost/sync": dial unix /data/litestream.sock: connect: connection refused`),
	}

	if _, ok := detectDormancyCandidate(verifications, now, 24*time.Hour, 2); ok {
		t.Fatal("expected no dormancy candidate when signatures differ")
	}
}

func failedVerificationAt(completedAt time.Time, errorMessage string) model.Verification {
	startedAt := completedAt.Add(-10 * time.Second)
	return model.Verification{
		StartedAt:    startedAt,
		CompletedAt:  &completedAt,
		Status:       "failed",
		CheckType:    "integrity",
		Passed:       false,
		ErrorMessage: errorMessage,
	}
}
