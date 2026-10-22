package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
			script += "echo 'head-marker'\ni=0; while [ $i -lt 1000 ]; do echo 'time=2026-10-22 level=DEBUG msg=\"s3 request\" s3.operation=GetObject s3.method=GET'; i=$((i+1)); done\n"
			if fail {
				script += "echo 'Error: decode database: unexpected EOF' >&2\nexit 7\n"
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
			if fail {
				got := reporting.ClassifyVerificationFailure("integrity", err.Error())
				if got.Stage != "restore" || got.Restore == nil || got.Signature != "restore_decode_error" {
					t.Fatalf("classification=%+v", got)
				}
			}
			if fail && (strings.Contains(err.Error(), "s3 request") || !strings.Contains(err.Error(), "decode database: unexpected EOF")) {
				t.Fatalf("error must contain only final failure: %v", err)
			}
			var restore reporting.VerificationStep
			for _, step := range v.validationSteps {
				if step.Name == "restore" {
					restore = step
				}
			}
			if len(restore.OutputTail) > 8*1024 || !strings.Contains(restore.OutputTail, "head-marker") || !restore.RestoreOutputTruncated {
				t.Fatalf("evidence length=%d truncated=%v", len(restore.OutputTail), restore.RestoreOutputTruncated)
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

func TestRestoreOutputCountsDiscardedReplans(t *testing.T) {
	var output restoreOutput
	input := restoreReplanMessage + "\n" + strings.Repeat("ordinary output\n", 10000)
	if _, err := output.Write([]byte(input)); err != nil {
		t.Fatal(err)
	}
	text := output.text()
	if output.replans != 1 || !output.truncated || !strings.Contains(text, restoreReplanMessage) {
		t.Fatalf("count=%d truncated=%v", output.replans, output.truncated)
	}
}

func TestReportedReplansSurviveSummaryRewrite(t *testing.T) {
	for _, scenario := range []string{"deadline", "fault guard", "success"} {
		t.Run(scenario, func(t *testing.T) {
			received := make(chan reporting.VerificationPayload, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload reporting.VerificationPayload
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				received <- payload
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			cfg := DefaultConfig()
			cfg.ControlBaseURL = server.URL
			result := VerificationResult{StartedAt: time.Now(), Summary: "verification passed", Status: "passed", Passed: true, Steps: []reporting.VerificationStep{
				{Name: "restore first.db", RestoreReplanCount: 2},
				{Name: "restore second.db", RestoreReplanCount: 3},
			}}
			v := NewVerifier(cfg)
			v.finalizeResult(&result)
			switch scenario {
			case "deadline":
				v.failValidationResult(context.Background(), &result, &verifierDeadlineError{phase: "restore", elapsed: time.Second})
			case "fault guard":
				result = failedS3FaultGuardResult(result, "fault guard failed", "fault not exercised")
			}
			reporter := NewReporter(cfg)
			if err := reporter.SendVerification(context.Background(), reporting.VerificationPayload{Summary: result.Summary, Steps: result.Steps}); err != nil {
				t.Fatal(err)
			}
			select {
			case payload := <-received:
				if !strings.HasSuffix(payload.Summary, "; restore re-plans: 5") || strings.Count(payload.Summary, "restore re-plans:") != 1 {
					t.Fatalf("summary=%q", payload.Summary)
				}
			default:
				t.Fatal("verification report not received")
			}
		})
	}
}

func TestRestoreOutputPreservesLongFinalError(t *testing.T) {
	var output restoreOutput
	final := "Error: decode header: " + strings.Repeat("detail ", 1000)
	input := "time=now level=DEBUG msg=first\n" + strings.Repeat("time=now level=DEBUG msg=read\n", 1000) + final
	for start := 0; start < len(input); start += 13 {
		if _, err := output.Write([]byte(input[start:min(start+13, len(input))])); err != nil {
			t.Fatal(err)
		}
	}
	evidence := output.text()
	if output.finalError != strings.TrimSpace(final) {
		t.Fatalf("final error length=%d want=%d", len(output.finalError), len(strings.TrimSpace(final)))
	}
	if len(evidence) > 8*1024 || !strings.HasPrefix(evidence, "time=now level=DEBUG msg=first") {
		t.Fatalf("invalid evidence length=%d", len(evidence))
	}
}

func TestRestoreOutputPreservesUnindentedErrorBlock(t *testing.T) {
	var output restoreOutput
	final := "Error: restore failed:\noperation error S3: GetObject\nStatusCode: 503"
	input := "time=now level=DEBUG msg=read\n" + final
	for _, line := range strings.SplitAfter(input, "\n") {
		if _, err := output.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	output.text()
	if output.finalError != final {
		t.Fatalf("final error=%q", output.finalError)
	}
	got := reporting.ClassifyVerificationFailure("restore", output.finalError)
	if got.Signature != "restore_s3_slowdown" {
		t.Fatalf("classification=%+v", got)
	}
}
