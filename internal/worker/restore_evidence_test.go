package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestVerifierRetainsRestoreReplans(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered", true: "failed"}[fail], func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig()
			cfg.DBPath = filepath.Join(dir, "source.db")
			cfg.LogicalTimeout = 30 * time.Second
			logicalTestDB(t, cfg.DBPath, "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
			logicalTestDB(t, cfg.DBPath+".restored", "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES(1)")
			startBoundarySyncFixture(t, &cfg, 42)
			script := "#!/bin/sh\nif [ \"$LOG_LEVEL\" != DEBUG ]; then echo debug-disabled; exit 9; fi\necho 'restore file disappeared, recalculating plan token=private' >&2\necho 'restore file disappeared, recalculating plan'\n"
			if fail {
				script += "echo 'restore plan retries exhausted: decode: file does not exist' >&2\nexit 7\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("LOG_LEVEL", "ERROR")
			v := NewVerifier(cfg)
			passed, err := v.validate(context.Background(), 42)
			if passed == fail || (err != nil) != fail {
				t.Fatalf("passed=%v err=%v", passed, err)
			}
			var restore reporting.VerificationStep
			for _, step := range v.validationSteps {
				if step.Name == "restore" {
					restore = step
				}
			}
			raw, err := json.Marshal(restore)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["restore_replan_count"] != float64(2) {
				t.Fatalf("missing replan count: %s", raw)
			}
			if !strings.Contains(restore.OutputTail, "recalculating plan") || strings.Contains(restore.OutputTail, "private") {
				t.Fatalf("restore output: %q", restore.OutputTail)
			}
			result := VerificationResult{StartedAt: time.Now(), Summary: "verification complete", Steps: v.validationSteps}
			v.finalizeResult(&result)
			if !strings.Contains(result.Summary, "restore re-plans: 2") {
				t.Fatalf("summary=%q", result.Summary)
			}
		})
	}
}

func TestRestoreOutputBoundAndStreamingCount(t *testing.T) {
	var output restoreOutput
	input := "token=private\n" + strings.Repeat("ordinary output\n", 10000) + "restore file disappeared, recalculating plan\n" + strings.Repeat("secret=private", 10000) + "\nrestore file disappeared, recalculating plan\nlast line"
	for start := 0; start < len(input); start += 7 {
		if _, err := output.Write([]byte(input[start:min(start+7, len(input))])); err != nil {
			t.Fatal(err)
		}
	}
	text := output.text()
	if len(text) > syncDiagnosticOutputLimit || !output.truncated {
		t.Fatalf("length=%d truncated=%v", len(text), output.truncated)
	}
	if output.replans != 2 {
		t.Fatalf("replans=%d", output.replans)
	}
	if strings.Contains(text, "private") || !strings.HasSuffix(text, "last line") {
		t.Fatalf("unsafe or missing tail: %q", text)
	}
}

func TestRestoreOutputRedactsCompleteLines(t *testing.T) {
	var output restoreOutput
	for _, chunk := range []string{"token=pri", "vate\nhttps://user:password@example.test/a?X-Amz-Signature=hidden\n", "secret=last"} {
		if _, err := output.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	text := output.text()
	for _, secret := range []string{"private", "user:password", "hidden", "last"} {
		if strings.Contains(text, secret) {
			t.Fatalf("unredacted %s: %q", secret, text)
		}
	}
	if output.truncated || output.replans != 0 {
		t.Fatalf("unexpected metadata: %+v", output)
	}
}

func TestVerificationSummaryAggregatesDatabaseReplans(t *testing.T) {
	result := VerificationResult{StartedAt: time.Now(), Summary: "verification passed", Steps: []reporting.VerificationStep{
		{Name: "restore first.db", RestoreReplanCount: 2},
		{Name: "restore second.db", RestoreReplanCount: 3},
	}}
	NewVerifier(DefaultConfig()).finalizeResult(&result)
	if result.Summary != "verification passed; restore re-plans: 5" {
		t.Fatalf("summary=%q", result.Summary)
	}
}

func TestRestoreOutputCountsDiscardedReplans(t *testing.T) {
	var output restoreOutput
	input := restoreReplanMessage + "\n" + strings.Repeat("ordinary output\n", 10000)
	if _, err := output.Write([]byte(input)); err != nil {
		t.Fatal(err)
	}
	text := output.text()
	if output.replans != 1 || !output.truncated || strings.Contains(text, restoreReplanMessage) {
		t.Fatalf("count=%d truncated=%v", output.replans, output.truncated)
	}
}
