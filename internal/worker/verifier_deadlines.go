package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

func verificationPhaseBudget(floor time.Duration, size int64) time.Duration {
	if floor <= 0 {
		floor = 30 * time.Minute
	}
	units := size / (1 << 30)
	if size%(1<<30) > 0 {
		units++
	}
	if units < 1 {
		units = 1
	}
	if units > math.MaxInt64/int64(floor) {
		return time.Duration(math.MaxInt64)
	}
	return floor * time.Duration(units)
}

func verificationCooldown(interval, elapsed time.Duration) time.Duration {
	if elapsed > time.Duration(math.MaxInt64/2) {
		return time.Duration(math.MaxInt64)
	}
	return max(interval, 2*elapsed)
}

func (v *Verifier) runValidationPhase(parent context.Context, name string, budget time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	result := VerificationResult{}
	err := recordVerificationStep(&result, name, func() error {
		started := time.Now()
		err := fn(ctx)
		if ctx.Err() == nil {
			return err
		}
		var metadata *verificationStepMetadataError
		if !errors.As(err, &metadata) {
			metadata = &verificationStepMetadataError{err: err}
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			metadata.err = &verifierDeadlineError{phase: name, elapsed: time.Since(started), signal: metadata.signal}
		} else {
			metadata.err = fmt.Errorf("%s: verifier canceled: %w", name, ctx.Err())
		}
		metadata.contextCanceled = true
		metadata.contextError = ctx.Err().Error()
		return metadata
	})
	deadline, _ := ctx.Deadline()
	deadline = deadline.UTC()
	result.Steps[0].DeadlineAt = &deadline
	v.validationSteps = append(v.validationSteps, result.Steps[0])
	return err
}

type verifierDeadlineError struct {
	phase   string
	elapsed time.Duration
	signal  string
}

func (e *verifierDeadlineError) Error() string {
	suffix := ""
	if e.signal != "" {
		suffix = " (" + e.signal + ")"
	}
	return fmt.Sprintf("%s: verifier deadline exceeded after %s%s", e.phase, e.elapsed.Round(time.Millisecond), suffix)
}

func (e *verifierDeadlineError) Unwrap() error { return context.DeadlineExceeded }

func (v *Verifier) failValidationResult(ctx context.Context, result *VerificationResult, err error) {
	v.failResult(ctx, result, err.Error())
	var deadline *verifierDeadlineError
	if errors.As(err, &deadline) && result.Status != "aborted" {
		result.Status = "aborted"
		result.Summary = "verification inconclusive: " + summarizeVerificationMessage(err.Error())
	}
}
