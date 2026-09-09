package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func retryFixture(t *testing.T, script string) (*pprofCapturer, string, string) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey = "bucket", "access", "secret"
	bin := t.TempDir()
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(bin, "s3cmd"), []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(cfg.DataDir, "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	remote := t.TempDir()
	t.Setenv("PROFILE_REMOTE", remote)
	return newPprofCapturer(&cfg), dir, remote
}

func pendingFixture(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(profileRecord{Artifact: name, Status: "available", Upload: "pending"})
	if err := os.WriteFile(filepath.Join(dir, name+".json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

const retryCopyScript = "for arg in \"$@\"; do previous=$last; last=$arg; done\ncp \"$previous\" \"$PROFILE_REMOTE/$(basename \"$last\")\"\n"

func TestPprofRetrySlowPairDelivery(t *testing.T) {
	c, dir, remote := retryFixture(t, "sleep 2\n"+retryCopyScript)
	pendingFixture(t, dir, "sample.pprof")
	c.retryPending(context.Background(), dir)
	for _, name := range []string{"sample.pprof", "sample.pprof.json"} {
		if _, err := os.Stat(filepath.Join(remote, name)); err != nil {
			t.Errorf("missing delivered %s: %v", name, err)
		}
	}
	if c.pending(filepath.Join(dir, "sample.pprof")) {
		t.Error("slow admissible pair remains pending")
	}
}

func TestPprofRetryFairAfterDeadline(t *testing.T) {
	c, dir, remote := retryFixture(t, "for arg in \"$@\"; do case \"$arg\" in *a.pprof) exec sleep 10;; esac; done\n"+retryCopyScript)
	pendingFixture(t, dir, "a.pprof")
	pendingFixture(t, dir, "b.pprof")
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c.retryPending(ctx, dir)
		cancel()
	}
	if _, err := os.Stat(filepath.Join(remote, "b.pprof.json")); err != nil {
		t.Fatal("later record starved behind timed-out first artifact:", err)
	}
}

func TestPprofUploadContextCause(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancelled"}[cancelled], func(t *testing.T) {
			c, dir, _ := retryFixture(t, "exec sleep 10\n")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			want := context.DeadlineExceeded
			if cancelled {
				cancel()
				want = context.Canceled
			}
			err := c.upload(ctx, filepath.Join(dir, "sample"), "sample")
			if !errors.Is(err, want) {
				t.Fatalf("lost context cause: got %v, want %v", err, want)
			}
		})
	}
}

func TestPprofRetryResumesManifestWithDurableFailure(t *testing.T) {
	c, dir, remote := retryFixture(t, "for arg in \"$@\"; do last=$arg; done\ncase \"$last\" in *.json) if [ ! -f \"$PROFILE_REMOTE/recover\" ]; then exec sleep 10; fi;; esac\n"+retryCopyScript)
	pendingFixture(t, dir, "sample.pprof")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	c.retryPending(ctx, dir)
	cancel()
	body, err := os.ReadFile(filepath.Join(dir, "sample.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if !record.ArtifactUploaded || record.UploadFailureCount != 1 || len(record.UploadFailures) != 1 || record.UploadFailures[0].Kind != "deadline_exceeded" || record.UploadFailures[0].Stage != "manifest" {
		t.Fatalf("lost failure classification: %s", body)
	}
	if err := os.WriteFile(filepath.Join(remote, "recover"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sample.pprof"), []byte("must not resend"), 0600); err != nil {
		t.Fatal(err)
	}
	c = newPprofCapturer(c.cfg)
	c.retryPending(context.Background(), dir)
	artifact, err := os.ReadFile(filepath.Join(remote, "sample.pprof"))
	if err != nil || string(artifact) != "evidence" {
		t.Fatalf("resent delivered artifact: %s, %v", artifact, err)
	}
	remoteBody, err := os.ReadFile(filepath.Join(remote, "sample.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	localBody, err := os.ReadFile(filepath.Join(dir, "sample.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(remoteBody) != string(localBody) {
		t.Fatal("remote recovery metadata differs")
	}
	if err := json.Unmarshal(remoteBody, &record); err != nil {
		t.Fatal(err)
	}
	if record.Upload != "uploaded" || record.UploadAttempts != 2 || record.UploadFailureCount != 1 || record.UploadFailures[0].Kind != "deadline_exceeded" || record.Error != "" {
		t.Fatalf("recovery lost upload failure or changed capture error: %s", remoteBody)
	}
}

func TestPprofRetryLimitsPassAndContinuesQueue(t *testing.T) {
	c, dir, remote := retryFixture(t, retryCopyScript)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		pendingFixture(t, dir, name+".pprof")
	}
	c.retryPending(context.Background(), dir)
	files, err := filepath.Glob(filepath.Join(remote, "*.json"))
	if err != nil || len(files) != 4 {
		t.Fatalf("pass did not bound deliveries: %v %v", files, err)
	}
	c.retryPending(context.Background(), dir)
	if _, err := os.Stat(filepath.Join(remote, "e.pprof.json")); err != nil {
		t.Fatal("next pass lost remaining delivery:", err)
	}
}

func TestPprofUploadFailureClassification(t *testing.T) {
	for _, tc := range []struct{ name, script, kind string }{
		{"exit", "exit 7\n", "subprocess_exit"},
		{"provider", "printf 'ERROR: S3 error: 403 AccessDenied %s\\n' \"$AWS_SECRET_ACCESS_KEY\" >&2\nexit 12\n", "provider_error"},
		{"killed", "kill -9 $$\n", "subprocess_exit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, dir, _ := retryFixture(t, tc.script)
			pendingFixture(t, dir, "sample.pprof")
			c.retryPending(context.Background(), dir)
			body, err := os.ReadFile(filepath.Join(dir, "sample.pprof.json"))
			if err != nil {
				t.Fatal(err)
			}
			var record profileRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			if len(record.UploadFailures) != 1 || record.UploadFailures[0].Kind != tc.kind || record.UploadFailures[0].ExitCode == nil {
				t.Fatalf("incorrect classification: %s", body)
			}
			if strings.Contains(string(body), c.cfg.S3SecretKey) {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestPprofUploadCancellationDuringCommand(t *testing.T) {
	c, dir, remote := retryFixture(t, "touch \"$PROFILE_REMOTE/started\"\nexec sleep 10\n")
	pendingFixture(t, dir, "sample.pprof")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.retryPending(ctx, dir); close(done) }()
	deadline := time.After(2 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(remote, "started")); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("command never started")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upload ignored shutdown cancellation")
	}
	body, err := os.ReadFile(filepath.Join(dir, "sample.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.UploadFailures) != 1 || record.UploadFailures[0].Kind != "cancelled" || record.Upload != "pending" {
		t.Fatalf("cancelled attempt not retained: %s", body)
	}
}

func TestPprofRetryOldFailuresProgressWithFreshArrivals(t *testing.T) {
	c, dir, _ := retryFixture(t, "exit 1\n")
	old := time.Now().Add(-time.Hour)
	write := func(name string, attempts uint64, at time.Time) {
		t.Helper()
		record := profileRecord{Upload: "pending", Status: "unavailable", UploadAttempts: attempts, CapturedAt: at}
		body, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".json"), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "z-old"} {
		write(name, 87, old)
	}
	for round := range 3 {
		for item := range 4 {
			write(fmt.Sprintf("fresh-%d-%d", round, item), 0, time.Now())
		}
		c = newPprofCapturer(c.cfg)
		c.retryPending(context.Background(), dir)
	}
	body, err := os.ReadFile(filepath.Join(dir, "z-old.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if record.UploadAttempts <= 87 {
		t.Fatal("historical failures penalized forever behind fresh arrivals")
	}
}
