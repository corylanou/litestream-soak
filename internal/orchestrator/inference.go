package orchestrator

import (
	"strings"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

// verificationFailure holds the result of classifying a verification failure.
// Classification is computed once per verification so rules change in one place.
type verificationFailure struct {
	Stage          string
	Signature      string
	Classification *reporting.FailureClassification
}

func classifyVerification(verification *model.Verification) verificationFailure {
	if verification == nil {
		return verificationFailure{}
	}
	return classifyFailureMessage(verification.CheckType, verification.ErrorMessage)
}

func classifyFailureMessage(checkType, errorMessage string) verificationFailure {
	classification := reporting.ClassifyVerificationFailure(checkType, errorMessage)
	return verificationFailure{
		Stage:          classification.Stage,
		Signature:      classification.Signature,
		Classification: &classification,
	}
}

func (f verificationFailure) probableSubsystem() string {
	return inferProbableSubsystem(f.Stage, f.Signature)
}

func inferFailureStage(verification *model.Verification) string {
	return classifyVerification(verification).Stage
}

func inferFailureSignature(verification *model.Verification) string {
	return classifyVerification(verification).Signature
}

func inferProbableSubsystem(stage, signature string) string {
	text := strings.ToLower(stage + " " + signature)
	switch {
	case strings.Contains(text, "disk_capacity") || strings.Contains(text, "disk_full"):
		return "Disk capacity / restore scratch headroom"
	case strings.Contains(text, "db_sync_executor") || strings.Contains(text, "db sync executor"):
		return "Litestream DB sync executor"
	case strings.Contains(text, "sync") || strings.Contains(text, "litestream_sync_socket_refused") || strings.Contains(text, "litestream_sync_timeout") || strings.Contains(text, "litestream_sync_fd_exhausted"):
		return "Litestream sync/control socket"
	case strings.Contains(text, "restore") || strings.Contains(text, "replica_") || strings.Contains(text, "ltx"):
		return "Replication or restore path"
	case strings.Contains(text, "integrity") || strings.Contains(text, "sqlite_index_mismatch") || strings.Contains(text, "validation_failed") || strings.Contains(text, "validation"):
		return "Restore correctness / integrity validation"
	case strings.Contains(text, "pause load") || strings.Contains(text, "checkpoint"):
		return "Soak harness or worker runtime"
	default:
		return "Needs operator triage"
	}
}

func activeFailure(verification *model.Verification) bool {
	if verification == nil {
		return false
	}
	return !verification.Passed || strings.EqualFold(verification.Status, "failed")
}
