package orchestrator

import (
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	defaultEnvEscalateConsecutive = 4
	defaultEnvEscalateDuration    = 30 * time.Minute
)

// EnvironmentalFailurePolicy decides when a transient provider-side object
// store error counts as environment weather instead of a worker failure.
// Bucket must be the configured shared replica bucket; an empty bucket
// disables environmental classification entirely (fail closed).
type EnvironmentalFailurePolicy struct {
	Bucket                   string
	EscalateAfterConsecutive int
	EscalateAfterDuration    time.Duration
}

func (p EnvironmentalFailurePolicy) normalized() EnvironmentalFailurePolicy {
	if p.EscalateAfterConsecutive <= 0 {
		p.EscalateAfterConsecutive = defaultEnvEscalateConsecutive
	}
	if p.EscalateAfterDuration <= 0 {
		p.EscalateAfterDuration = defaultEnvEscalateDuration
	}
	return p
}

var environmentalFailurePolicy EnvironmentalFailurePolicy

// ConfigureEnvironmentalFailurePolicy installs the process-wide policy at
// startup, mirroring the env-flag pattern used by fleet gating so every
// consumer (ingest guard, home context, deployment scorecards) applies the
// same rules.
func ConfigureEnvironmentalFailurePolicy(policy EnvironmentalFailurePolicy) {
	environmentalFailurePolicy = policy.normalized()
}

func currentEnvironmentalFailurePolicy() EnvironmentalFailurePolicy {
	return environmentalFailurePolicy.normalized()
}

// isTransientObjectStoreFailure reports whether a classified failure is a
// provider blip on the KNOWN configured bucket: NoSuchBucket/404 on
// ListObjectsV2 (the 2026-07-08 Tigris incident) or SlowDown/503 throttling.
// A bucket mismatch is never environmental — a genuinely misconfigured or
// deleted bucket must keep paging.
func isTransientObjectStoreFailure(classification *reporting.FailureClassification, policy EnvironmentalFailurePolicy) bool {
	if classification == nil || classification.ObjectStore == nil {
		return false
	}
	configured := strings.TrimSpace(policy.Bucket)
	if configured == "" {
		return false
	}
	failure := classification.ObjectStore
	if bucket := strings.TrimSpace(failure.Bucket); bucket != "" && bucket != configured {
		return false
	}
	operation := strings.ToLower(failure.Operation)
	apiCode := strings.ToLower(failure.APICode)
	switch {
	case apiCode == "nosuchbucket", operation == "listobjectsv2" && failure.HTTPStatus == 404:
		return true
	case apiCode == "slowdown", failure.HTTPStatus == 503:
		return true
	default:
		return false
	}
}

// environmentalStreak marks one verification inside a worker's history as
// escalated when the transient-error streak it belongs to has run too long:
// more than EscalateAfterConsecutive consecutive environmental failures, or
// spanning more than EscalateAfterDuration. Streaks reset on any pass or any
// non-environmental failure.
type environmentalStreak struct {
	count int
	start time.Time
}

func (s *environmentalStreak) observe(startedAt time.Time) {
	if s.count == 0 {
		s.start = startedAt
	}
	s.count++
}

func (s *environmentalStreak) reset() {
	s.count = 0
	s.start = time.Time{}
}

func (s environmentalStreak) escalated(at time.Time, policy EnvironmentalFailurePolicy) bool {
	if s.count > policy.EscalateAfterConsecutive {
		return true
	}
	return s.count > 0 && at.Sub(s.start) > policy.EscalateAfterDuration
}

// escalatedEnvironmentalStatIDs walks each worker's verification history in
// order and returns the IDs of environmental failures whose streak breached
// the escalation thresholds — those must be treated as real failures again.
func escalatedEnvironmentalStatIDs(stats []model.VerificationStat, policy EnvironmentalFailurePolicy) map[int]bool {
	byWorker := make(map[string][]model.VerificationStat)
	for _, stat := range stats {
		byWorker[stat.WorkerID] = append(byWorker[stat.WorkerID], stat)
	}

	escalated := make(map[int]bool)
	for _, workerStats := range byWorker {
		sort.Slice(workerStats, func(i, j int) bool {
			return workerStats[i].StartedAt.Before(workerStats[j].StartedAt)
		})
		var streak environmentalStreak
		for _, stat := range workerStats {
			verification := verificationFromStat(stat)
			if !verification.Failed() {
				streak.reset()
				continue
			}
			failure := classifyFailureMessage(stat.CheckType, stat.ErrorMessage)
			if !isTransientObjectStoreFailure(failure.Classification, policy) {
				streak.reset()
				continue
			}
			streak.observe(stat.StartedAt)
			if streak.escalated(stat.StartedAt, policy) {
				escalated[stat.ID] = true
			}
		}
	}
	return escalated
}

// environmentalStreakEscalated answers the same question for a single worker
// at ingest time: given the latest verifications (newest first) and one more
// environmental failure arriving now, has the streak breached the thresholds?
func environmentalStreakEscalated(previous []model.Verification, now time.Time, policy EnvironmentalFailurePolicy) bool {
	streak := environmentalStreak{count: 1, start: now}
	for _, verification := range previous {
		if !verification.Failed() {
			break
		}
		failure := classifyVerification(&verification)
		if !isTransientObjectStoreFailure(failure.Classification, policy) {
			break
		}
		streak.count++
		streak.start = verification.StartedAt
	}
	return streak.escalated(now, policy)
}

// environmentalWithoutEscalation is the ingest-time guard: a transient
// provider blip on the configured bucket keeps the worker out of degraded —
// unless the streak has run long enough that it must page again.
func (a *API) environmentalWithoutEscalation(workerID string, failure verificationFailure, startedAt time.Time) bool {
	policy := currentEnvironmentalFailurePolicy()
	if !isTransientObjectStoreFailure(failure.Classification, policy) {
		return false
	}
	previous, err := a.db.ListVerifications(workerID, policy.EscalateAfterConsecutive+1)
	if err != nil {
		return false
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	return !environmentalStreakEscalated(previous, startedAt, policy)
}
