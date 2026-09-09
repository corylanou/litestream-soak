package recovery

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunRejectsUnpinnedBinary(t *testing.T) {
	_, err := Run(context.Background(), Config{Binary: os.Args[0], Output: filepath.Join(t.TempDir(), "result")})
	if err == nil {
		t.Fatal("accepted missing binary pin")
	}
}

func TestRecoveryPinnedBinary(t *testing.T) {
	binary := os.Getenv("SOAK_RECOVERY_BINARY")
	if binary == "" {
		t.Skip("opt-in: SOAK_RECOVERY_BINARY and SOAK_RECOVERY_SHA256")
	}
	output := os.Getenv("SOAK_RECOVERY_OUTPUT")
	if output == "" {
		output = filepath.Join(t.TempDir(), "recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	r, err := Run(ctx, Config{Binary: binary, SHA256: os.Getenv("SOAK_RECOVERY_SHA256"), Output: output, Window: 20 * time.Second})
	if err != nil {
		t.Fatalf("run: %v; evidence %s", err, output)
	}
	for _, name := range []string{"process-kill", "restart", "interrupted-restore", "local-loss", "active-retention", "pit", "retention-boundary", "follow-resume"} {
		found := false
		for _, a := range r.Attempts {
			if a.Name == name {
				found = true
			}
		}
		if !found {
			t.Errorf("missing scenario %s", name)
		}
	}
	if r.Status != "passed" {
		t.Fatalf("status=%s; evidence %s", r.Status, output)
	}
}
