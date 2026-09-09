package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPprofCapturerSkipsWhenDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join(t.TempDir(), "litestream.sock")
	cfg.PprofCaptureEnabled = false

	done := make(chan struct{})
	go func() {
		newPprofCapturer(&cfg).Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pprof capturer did not return when capture is disabled")
	}

	if _, err := os.Stat(filepath.Join(cfg.DataDir, "profiles")); !os.IsNotExist(err) {
		t.Fatalf("profiles dir stat error = %v, want not exists", err)
	}
}

func TestPprofCapturerRunsForSingleDBWorkers(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join("/tmp", fmt.Sprintf("litestream-soak-singledb-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socketPath) })

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "profile")
	})
	startTestUnixServer(t, socketPath, mux)

	cfg := DefaultConfig() // single database, capture enabled by default
	cfg.DataDir = dir
	cfg.SocketPath = socketPath
	if cfg.ManyDBEnabled() {
		t.Fatal("default config unexpectedly enables many-DB mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		newPprofCapturer(&cfg).Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(dir, "profiles"))
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		if slices.ContainsFunc(names, func(n string) bool { return strings.Contains(n, "_baseline_heap.") }) &&
			slices.ContainsFunc(names, func(n string) bool { return strings.Contains(n, "_baseline_memstats.") }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("baseline captures not written for single-DB worker, got %v", names)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pprof capturer did not stop after cancel")
	}
}

func TestPprofCapturerCapturesMemStatsAsText(t *testing.T) {
	dir := t.TempDir()
	socketPath := filepath.Join("/tmp", fmt.Sprintf("litestream-soak-memstats-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socketPath) })

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("debug") == "1" {
			_, _ = io.WriteString(w, "heap profile: 0: 0 [0: 0]\n\n# runtime.MemStats\n# StackInuse = 5668864\n# StackSys = 5668864\n")
			return
		}
		_, _ = io.WriteString(w, "binary-heap-profile")
	})
	mux.HandleFunc("/debug/pprof/allocs", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "binary-allocs-profile")
	})
	mux.HandleFunc("/debug/pprof/goroutine", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "goroutine profile")
	})
	startTestUnixServer(t, socketPath, mux)

	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.SocketPath = socketPath

	newPprofCapturer(&cfg).captureSet(context.Background(), "baseline")

	entries, err := os.ReadDir(filepath.Join(dir, "profiles"))
	if err != nil {
		t.Fatalf("read profiles dir: %v", err)
	}
	var memstatsFile, heapFile string
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, "_memstats.txt"):
			memstatsFile = name
		case strings.HasSuffix(name, "_heap.pprof"):
			heapFile = name
		}
	}
	if heapFile == "" {
		t.Fatalf("expected a binary heap.pprof capture, got entries: %v", entries)
	}
	if memstatsFile == "" {
		t.Fatalf("expected a memstats.txt capture, got entries: %v", entries)
	}
	body, err := os.ReadFile(filepath.Join(dir, "profiles", memstatsFile))
	if err != nil {
		t.Fatalf("read memstats capture: %v", err)
	}
	if !strings.Contains(string(body), "StackInuse") {
		t.Fatalf("memstats capture missing StackInuse, got: %q", body)
	}
}

func TestPprofCapturerPrunesOldLocalProfiles(t *testing.T) {
	cfg := DefaultConfig()
	capturer := newPprofCapturer(&cfg)
	dir := t.TempDir()
	names := []string{
		"20260101T000000Z_baseline_heap.pprof",
		"20260101T010000Z_hourly_heap.pprof",
		"20260101T020000Z_hourly_heap.pprof",
		"20260101T030000Z_hourly_heap.pprof",
		"20260101T040000Z_hourly_heap.pprof",
	}
	for i, name := range names {
		target := filepath.Join(dir, name)
		if err := os.WriteFile(target, []byte("profile"), 0o644); err != nil {
			t.Fatal(err)
		}
		modTime := time.Unix(int64(i), 0)
		if err := os.Chtimes(target, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	capturer.pruneLocalProfiles(dir, 2)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	slices.Sort(got)

	want := []string{
		"20260101T000000Z_baseline_heap.pprof",
		"20260101T030000Z_hourly_heap.pprof",
		"20260101T040000Z_hourly_heap.pprof",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("remaining profiles = %v, want %v", got, want)
	}
}

func TestPprofCapturerPrunesOldBaselines(t *testing.T) {
	cfg := DefaultConfig()
	capturer := newPprofCapturer(&cfg)
	dir := t.TempDir()
	names := []string{
		"20260101T000000Z_baseline_heap.pprof",
		"20260101T000001Z_baseline_allocs.pprof",
		"20260101T000002Z_baseline_goroutine.txt",
		"20260101T000003Z_baseline_memstats.txt",
		"20260102T000000Z_baseline_heap.pprof",
		"20260102T000001Z_baseline_allocs.pprof",
		"20260102T000002Z_baseline_goroutine.txt",
		"20260102T000003Z_baseline_memstats.txt",
		"20260102T010000Z_hourly_heap.pprof",
	}
	for i, name := range names {
		target := filepath.Join(dir, name)
		if err := os.WriteFile(target, []byte("profile"), 0o644); err != nil {
			t.Fatal(err)
		}
		modTime := time.Unix(int64(i), 0)
		if err := os.Chtimes(target, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	capturer.pruneLocalProfiles(dir, 96)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	slices.Sort(got)

	want := []string{
		"20260102T000000Z_baseline_heap.pprof",
		"20260102T000001Z_baseline_allocs.pprof",
		"20260102T000002Z_baseline_goroutine.txt",
		"20260102T000003Z_baseline_memstats.txt",
		"20260102T010000Z_hourly_heap.pprof",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("remaining profiles = %v, want %v", got, want)
	}
}

func TestPprofStartupCPUAndIncident(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	requests := make(chan string, 30)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.String()
		_, _ = io.WriteString(w, "evidence")
	}))
	c := newPprofCapturer(&cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	waitCPU := func() {
		t.Helper()
		for {
			select {
			case request := <-requests:
				if strings.Contains(request, "profile?seconds=5") {
					return
				}
			case <-time.After(2 * time.Second):
				t.Fatal("missing short CPU capture")
			}
		}
	}
	waitCPU()
	c.Trigger("verification-failed")
	deadline := time.Now().Add(2 * time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*_verification-failed_heap.pprof.json"))
		if len(files) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing incident artifact")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestPprofCaptureRecordsFailureAndIdentity(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	cfg.LitestreamSHA = "candidate-sha"
	cfg.RunID = "run-213"
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unsupported", 404) }))
	newPprofCapturer(&cfg).captureEndpoint(context.Background(), "incident", "mutex", "mutex", time.Second)
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*.json"))
	if len(files) != 1 {
		t.Fatalf("failure manifests = %v", files)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"candidate-sha", "run-213", "incident", "404", "unavailable"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("manifest missing %q: %s", want, body)
		}
	}
}

func TestPprofUploadTransport(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3AccessKey = "example-key"
	cfg.S3SecretKey = "example-secret"
	cfg.S3Endpoint = "http://127.0.0.1:9000"
	dir := t.TempDir()
	output := filepath.Join(dir, "args")
	t.Setenv("PROFILE_TEST_ARGS", output)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PROFILE_TEST_ARGS\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := newPprofCapturer(&cfg).upload(context.Background(), "/tmp/example.pprof", "example.pprof"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--no-ssl", "--host-bucket=127.0.0.1:9000"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("args missing %s: %s", want, body)
		}
	}
	if strings.Contains(string(body), cfg.S3SecretKey) {
		t.Fatal("credentials exposed in arguments")
	}
}

func TestPprofRetainsFailedUploadAndRetries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey = "bucket", "key", "secret"
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "evidence") }))
	dir := t.TempDir()
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	executable := filepath.Join(dir, "s3cmd")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c := newPprofCapturer(&cfg)
	c.captureEndpoint(context.Background(), "incident", "heap", "heap", time.Second)
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*.pprof"))
	if len(files) != 1 || !c.pending(files[0]) {
		t.Fatalf("missing pending evidence: %v", files)
	}
	for i := 0; i < 3; i++ {
		c.captureEndpoint(context.Background(), "hourly", "heap", "heap", time.Second)
	}
	c.pruneLocalProfiles(filepath.Join(cfg.DataDir, "profiles"), 1)
	if _, err := os.Stat(files[0]); err != nil {
		t.Fatalf("failed upload was pruned: %v", err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c.retryPending(context.Background(), filepath.Join(cfg.DataDir, "profiles"))
	if c.pending(files[0]) {
		t.Fatal("successful retry still pending")
	}
}

func TestPprofBoundsEvidenceAndRejectsOversize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", pprofMaxCaptureBytes+1))
	}))
	c := newPprofCapturer(&cfg)
	c.captureEndpoint(context.Background(), "incident", "heap", "heap", time.Second)
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*.pprof"))
	if len(files) != 0 {
		t.Fatal("oversized evidence accepted")
	}
	dir := filepath.Join(cfg.DataDir, "profiles")
	for i := 0; i < pprofMaxEvidenceFiles-2; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if c.captureSpaceAvailable(dir) {
		t.Fatal("full storage admitted capture")
	}
}

func TestPprofOptionalEndpointsAndFinal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	requests := make(chan string, 50)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path
		_, _ = io.WriteString(w, "evidence")
	}))
	c := newPprofCapturer(&cfg)
	c.captureSet(context.Background(), "hourly")
	closeRequests := func() []string {
		var got []string
		for len(requests) > 0 {
			got = append(got, <-requests)
		}
		return got
	}
	for _, request := range closeRequests() {
		if strings.Contains(request, "mutex") || strings.Contains(request, "block") || strings.Contains(request, "trace") {
			t.Fatal("optional endpoint enabled by default")
		}
	}
	for _, kind := range []string{"BLOCK", "MUTEX", "TRACE"} {
		t.Setenv("SOAK_PPROF_"+kind, "true")
	}
	c.captureSet(context.Background(), "incident")
	got := closeRequests()
	for _, name := range []string{"block", "mutex", "trace"} {
		if !slices.Contains(got, "/debug/pprof/"+name) {
			t.Errorf("missing opt-in %s", name)
		}
	}
	c.captureSet(context.Background(), "final")
	if slices.Contains(closeRequests(), "/debug/pprof/profile") {
		t.Fatal("shutdown started CPU capture")
	}
}

func TestPprofStartupIgnoresSlowUploader(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey = "bucket", "key", "secret"
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	cpu := make(chan struct{}, 1)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/pprof/profile" {
			cpu <- struct{}{}
		}
		_, _ = io.WriteString(w, "evidence")
	}))
	dir := t.TempDir()
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(dir, "s3cmd"), []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { newPprofCapturer(&cfg).Run(ctx); close(done) }()
	select {
	case <-cpu:
	case <-time.After(time.Second):
		t.Fatal("uploader delayed startup CPU")
	}
	deadline := time.Now().Add(time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*_startup_cpu_profile.pprof.json"))
		if len(files) == 1 {
			body, err := os.ReadFile(files[0])
			if err != nil {
				t.Fatal(err)
			}
			var record profileRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			if record.Status != "available" {
				t.Fatalf("CPU not collected: %s", record.Status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slow uploader prevented CPU artifact")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

func TestPprofFinalWaitHonorsDeadlineAndPersistsStatus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	c := newPprofCapturer(&cfg)
	c.gate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	c.captureSet(ctx, "final")
	if time.Since(started) > time.Second {
		t.Fatal("capture lock ignored deadline")
	}
	for i := 0; i < 20; i++ {
		c.Trigger("incident")
	}
	body, err := os.ReadFile(filepath.Join(cfg.DataDir, "profiles", "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"cancelled-waiting", "queue-full"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing status %s", want)
		}
	}
}

func TestPprofLifecycleWritesFinalEvidence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "evidence") }))
	runner := NewRunner(cfg)
	stop := runner.startProfileCapture(context.Background())
	stop()
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*_final_memstats.txt.json"))
	if len(files) != 1 {
		t.Fatalf("missing final text evidence: %v", files)
	}
}

func TestPprofMetadataUsesIndependentIdentities(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LitestreamSHA, cfg.GitSHA = "candidate", "harness"
	cfg.ReplayDataURL = "https://example.com/data?token=private-example"
	cfg.WorkloadSHA = "generator"
	cfg.WorkloadID = "workload-identity"
	cfg.DeploymentID = 213
	t.Setenv("WORKLOAD_SHA", "different-environment")
	t.Setenv("SOAK_WORKLOAD_ID", "different-environment")
	c := newPprofCapturer(&cfg)
	body, err := json.Marshal(c.newRecord("startup", "heap", "example.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"candidate", "generator", "soak-verifier:harness", logicalValidatorVersion, "workload-identity", `"deployment_id":213`, profileHash(cfg.WorkloadConfig().JSON())} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing identity %s", want)
		}
	}
	if strings.Contains(string(body), "private-example") {
		t.Fatal("credential URL leaked")
	}
}

func TestPprofCPUExclusivityAndCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	entered := make(chan struct{}, 2)
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/pprof/profile" {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "evidence")
	}))
	c := newPprofCapturer(&cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.captureSet(ctx, "startup"); close(done) }()
	<-entered
	second, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	c.captureSet(second, "incident")
	select {
	case <-entered:
		t.Fatal("simultaneous CPU captures")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capture ignored cancellation")
	}
}

func TestPprofEmptyOptionalEndpointIsNotCollection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	newPprofCapturer(&cfg).captureEndpoint(context.Background(), "incident", "mutex", "mutex", time.Second)
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*.json"))
	if len(files) != 1 {
		t.Fatal("missing manifest")
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if !record.EndpointAvailable || record.Status == "available" || !strings.Contains(record.Sampling, "unverified") {
		t.Fatalf("unsupported collection misrepresented: %+v", record)
	}
}

func TestPprofSyncRecoveryRetainsIncidentTriggers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SocketPath = filepath.Join("/tmp", fmt.Sprintf("pprof-%d.sock", time.Now().UnixNano()))
	cfg.VerifySyncDegradedAfter = time.Nanosecond
	calls := 0
	startTestUnixServer(t, cfg.SocketPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync" {
			http.NotFound(w, r)
			return
		}
		calls++
		replicated := 0
		if calls > 1 {
			replicated = 1
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","txid":1,"replicated_txid":%d}`, replicated)
	}))
	v := NewVerifier(cfg)
	v.syncRetryDelay = time.Millisecond
	var incidents []string
	v.onProfileIncident = func(label string) { incidents = append(incidents, label) }
	if err := v.waitForSync(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(incidents, []string{"sync-degraded", "sync-recovered"}) {
		t.Fatalf("lost recovered incident: %v", incidents)
	}
}
