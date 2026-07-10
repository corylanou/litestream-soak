package orchestrator

import (
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	failureCategoryActionable    = "actionable"
	failureCategoryEnvironmental = "environmental"
	failureCategoryRampUp        = "ramp-up"
	failureSeverityBad           = "bad"
	failureSeverityWarn          = "warn"
	rampUpFailureDeadline        = 90 * time.Minute
	s3CorrelationWindow          = 4 * time.Hour
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
	case strings.Contains(text, "s3_bucket_missing") || strings.Contains(text, "s3_slowdown"):
		return "Object store provider (environmental)"
	case strings.Contains(text, "s3_transport") || strings.Contains(text, "s3-transport"):
		return "S3 transport"
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
	return verification.Failed()
}

type failureClassificationContext struct {
	categoryByVerificationID map[int]string
	environmentalSources     map[string]map[string]time.Time
}

func buildFailureClassificationContext(stats []model.VerificationStat) failureClassificationContext {
	ctx := failureClassificationContext{
		categoryByVerificationID: make(map[int]string),
		environmentalSources:     make(map[string]map[string]time.Time),
	}

	policy := currentEnvironmentalFailurePolicy()
	escalated := escalatedEnvironmentalStatIDs(stats, policy)
	for _, stat := range stats {
		verification := verificationFromStat(stat)
		if !verification.Failed() {
			continue
		}
		failure := classifyFailureMessage(stat.CheckType, stat.ErrorMessage)
		category := failureCategoryActionable
		switch {
		case isTransientObjectStoreFailure(failure.Classification, policy) && !escalated[stat.ID]:
			category = failureCategoryEnvironmental
			ctx.addEnvironmentalSource(failure.Signature, stat.Source, stat.StartedAt)
		case failure.Signature == "s3_transport" && hasCorrelatedS3TransportFailure(stat, stats):
			category = failureCategoryEnvironmental
			ctx.addEnvironmentalSource(failure.Signature, stat.Source, stat.StartedAt)
		case isRampUpFailureStat(stat):
			category = failureCategoryRampUp
		}
		ctx.categoryByVerificationID[stat.ID] = category
	}

	return ctx
}

func (ctx failureClassificationContext) categoryForVerificationID(id int) string {
	if ctx.categoryByVerificationID == nil {
		return failureCategoryActionable
	}
	return firstNonEmpty(ctx.categoryByVerificationID[id], failureCategoryActionable)
}

func (ctx *failureClassificationContext) addEnvironmentalSource(signature, source string, occurredAt time.Time) {
	signature = strings.TrimSpace(signature)
	source = strings.TrimSpace(source)
	if signature == "" || source == "" {
		return
	}
	if ctx.environmentalSources[signature] == nil {
		ctx.environmentalSources[signature] = make(map[string]time.Time)
	}
	if occurredAt.After(ctx.environmentalSources[signature][source]) {
		ctx.environmentalSources[signature][source] = occurredAt
	}
}

// recentEnvironmentalSignatures lists signatures whose newest environmental
// failure landed within the window: the attention banner reports live
// provider weather, not incidents that already blew over (those age out of
// the KPI tiles on their own).
func (ctx failureClassificationContext) recentEnvironmentalSignatures(now time.Time, window time.Duration) []string {
	signatures := make([]string, 0, len(ctx.environmentalSources))
	for signature, sources := range ctx.environmentalSources {
		for _, occurredAt := range sources {
			if now.Sub(occurredAt) <= window {
				signatures = append(signatures, signature)
				break
			}
		}
	}
	sort.Strings(signatures)
	return signatures
}

func (ctx failureClassificationContext) environmentalSourceLabels(signature string) []string {
	sources := ctx.environmentalSources[strings.TrimSpace(signature)]
	if len(sources) == 0 {
		return nil
	}
	labels := make([]string, 0, len(sources))
	for source := range sources {
		labels = append(labels, sourceHumanLabel(source))
	}
	sort.Strings(labels)
	return labels
}

func failureSeverityForCategory(category string) string {
	switch category {
	case failureCategoryEnvironmental, failureCategoryRampUp:
		return failureSeverityWarn
	default:
		return failureSeverityBad
	}
}

func verificationFromStat(stat model.VerificationStat) model.Verification {
	return model.Verification{
		ID:           stat.ID,
		WorkerID:     stat.WorkerID,
		StartedAt:    stat.StartedAt,
		Status:       stat.Status,
		CheckType:    stat.CheckType,
		Passed:       stat.Passed,
		DurationMS:   stat.DurationMS,
		ErrorMessage: stat.ErrorMessage,
	}
}

func isRampUpFailureStat(stat model.VerificationStat) bool {
	if stat.HasPriorPass || stat.WorkerCreatedAt.IsZero() {
		return false
	}
	createdAt := stat.WorkerCreatedAt.UTC()
	return !stat.StartedAt.Before(createdAt) && stat.StartedAt.Before(createdAt.Add(rampUpFailureDeadline))
}

func hasCorrelatedS3TransportFailure(stat model.VerificationStat, stats []model.VerificationStat) bool {
	if strings.TrimSpace(stat.Source) == "" {
		return false
	}
	for _, other := range stats {
		if other.ID == stat.ID || strings.TrimSpace(other.Source) == "" || stat.Source == other.Source {
			continue
		}
		if !mainAndNonMainSources(stat.Source, other.Source) {
			continue
		}
		verification := verificationFromStat(other)
		if !verification.Failed() {
			continue
		}
		if classifyFailureMessage(other.CheckType, other.ErrorMessage).Signature != "s3_transport" {
			continue
		}
		if absDuration(stat.StartedAt.Sub(other.StartedAt)) <= s3CorrelationWindow {
			return true
		}
	}
	return false
}

func mainAndNonMainSources(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return (left == "main" && right != "main") || (left != "main" && right == "main")
}

func absDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return -duration
	}
	return duration
}
