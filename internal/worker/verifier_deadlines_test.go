package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestVerifierRestoreDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, script, want string
		inconclusive       bool
	}{
		{"timeout", "exec sleep 10", "restore: verifier deadline exceeded after", true},
		{"restore error", "echo broken replica; exit 7", "validation failed (exit 7): broken replica", false},
		{"timeout text in output", "echo verifier deadline exceeded after; exit 7", "validation failed (exit 7):", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig()
			cfg.DataDir = dir
			cfg.DBPath = filepath.Join(dir, "source.db")
			cfg.LogicalTimeout = time.Second
			if !tc.inconclusive {
				cfg.LogicalTimeout = 30 * time.Second
			}
			logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER)")
			startBoundarySyncFixture(t, &cfg, 42)
			if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte("#!/bin/sh\n"+tc.script+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			v := NewVerifier(cfg)
			passed, err := v.validate(context.Background(), 42)
			if passed || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("passed=%v err=%v", passed, err)
			}
			result := VerificationResult{StartedAt: time.Now()}
			v.failValidationResult(context.Background(), &result, err)
			record := model.Verification{Status: result.Status, Passed: result.Passed}
			if record.Inconclusive() != tc.inconclusive || record.Failed() == tc.inconclusive {
				t.Fatalf("status=%s", result.Status)
			}
			if tc.inconclusive && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline cause: %v", err)
			}
		})
	}
}

func TestVerificationPhaseBudget(t *testing.T) {
	for _, tc := range []struct {
		size int64
		want time.Duration
	}{
		{0, 30 * time.Minute}, {1 << 30, 30 * time.Minute}, {(1 << 30) + 1, time.Hour}, {8 << 30, 4 * time.Hour},
	} {
		if got := verificationPhaseBudget(30*time.Minute, tc.size); got != tc.want {
			t.Fatalf("size=%d got=%s want=%s", tc.size, got, tc.want)
		}
	}
}

func TestVerificationPhasesHaveIndependentDeadlines(t *testing.T) {
	v := NewVerifier(DefaultConfig())
	var previous time.Time
	for _, name := range []string{"source_snapshot", "restore", "integrity_check", "compare"} {
		err := v.runValidationPhase(context.Background(), name, time.Second, func(ctx context.Context) error {
			deadline, _ := ctx.Deadline()
			if !previous.IsZero() && !deadline.After(previous) {
				t.Fatal("shared deadline")
			}
			previous = deadline
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, step := range v.validationSteps {
		if step.DeadlineAt == nil || step.Status != "ok" {
			t.Fatalf("missing metadata: %+v", step)
		}
	}
	if len(v.validationSteps) != 4 {
		t.Fatal("missing phase records")
	}
}

func TestVerificationCooldown(t *testing.T) {
	if got := verificationCooldown(30*time.Minute, 40*time.Minute); got != 80*time.Minute {
		t.Fatalf("cooldown=%s", got)
	}
	if got := verificationCooldown(30*time.Minute, time.Minute); got != 30*time.Minute {
		t.Fatalf("floor=%s", got)
	}
}

func TestRunCycleRestoreDeadlineIsInconclusive(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "source.db")
	cfg.LogicalTimeout = time.Second
	logicalTestDB(t, cfg.DBPath, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER)")
	startBoundarySyncFixture(t, &cfg, 42)
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	pauser := &fakePauser{}
	result, err := NewVerifier(cfg, pauser).RunCycle(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != "aborted" || result.Passed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if pauser.resumeCalls != 1 {
		t.Fatalf("load not resumed: %d", pauser.resumeCalls)
	}
	var found bool
	for _, step := range result.Steps {
		if step.Name == "restore" {
			found = true
			if step.DeadlineAt == nil || step.Signal == "" || step.ContextError != context.DeadlineExceeded.Error() || len(step.Command) == 0 {
				t.Fatalf("missing timeout metadata: %+v", step)
			}
		}
	}
	if !found {
		t.Fatal("restore phase not reported")
	}
}

func TestValidationPhaseCancellation(t *testing.T) {
	for _, name := range []string{"source_snapshot", "restore", "integrity_check", "compare"} {
		t.Run(name, func(t *testing.T) {
			v := NewVerifier(DefaultConfig())
			err := v.runValidationPhase(context.Background(), name, time.Millisecond, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
			if !errors.Is(err, context.DeadlineExceeded) || !strings.HasPrefix(err.Error(), name+": verifier deadline") {
				t.Fatalf("phase error=%v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = v.runValidationPhase(ctx, name, time.Second, func(ctx context.Context) error { return ctx.Err() })
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation=%v", err)
			}
		})
	}
}

func TestValidationPhasePreservesCompletedRestoreFailure(t *testing.T) {
	v := NewVerifier(DefaultConfig())
	exitCode := 7
	failure := &verificationStepMetadataError{
		err:        errors.New("validation failed (exit 7): broken replica"),
		exitCode:   &exitCode,
		outputTail: "broken replica",
	}
	err := v.runValidationPhase(context.Background(), "restore", time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return failure
	})
	result := VerificationResult{StartedAt: time.Now()}
	v.failValidationResult(context.Background(), &result, err)
	if result.Status != "failed" || !strings.Contains(result.ErrorMessage, "validation failed (exit 7): broken replica") {
		t.Fatalf("completed restore failure became inconclusive: %+v", result)
	}
	if step := v.validationSteps[0]; step.ExitCode == nil || *step.ExitCode != 7 || step.OutputTail != "broken replica" {
		t.Fatalf("lost restore failure metadata: %+v", step)
	}
}

func TestValidationReservationFailureIsInconclusive(t *testing.T) {
	v := NewVerifier(DefaultConfig())
	result := VerificationResult{StartedAt: time.Now()}
	err := errors.New("source boundary unavailable: acquire writer reservation: context deadline exceeded")
	v.failValidationResult(context.Background(), &result, err)
	record := model.Verification{Status: result.Status, Passed: result.Passed}
	if result.Status != "pending" || !record.Inconclusive() || record.Failed() {
		t.Fatalf("reservation failure became actionable: %+v", result)
	}
}
