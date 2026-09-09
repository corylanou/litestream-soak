package worker

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
)

func TestFTSOperationProfile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.LitestreamSHA = "candidate"
	cfg.RunID = "fts-profile-test"
	capturer := newPprofCapturer(&cfg)
	finish := capturer.beginFTSProfile(context.Background(), "merge")
	db, err := openFTS(context.Background(), filepath.Join(cfg.DataDir, "fts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for range 8 {
		if _, err := stepFTS(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	finish()
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*worker_cpu.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("profile metadata=%v", files)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "available" || record.Phase != "fts-merge-during" || record.RunID != cfg.RunID {
		t.Fatalf("record=%+v", record)
	}
	artifact, err := os.ReadFile(filepath.Join(cfg.DataDir, "profiles", record.Artifact))
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact) < 2 || artifact[0] != 0x1f || artifact[1] != 0x8b {
		t.Fatal("not a compressed CPU profile")
	}
}

func TestFTSProfileConflictIsUnavailable(t *testing.T) {
	if err := pprof.StartCPUProfile(io.Discard); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	capture := newPprofCapturer(&cfg)
	capture.beginFTSProfile(context.Background(), "optimize")()
	files, err := filepath.Glob(filepath.Join(cfg.DataDir, "profiles", "*worker_cpu.pprof.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("records=%v", files)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var record profileRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "unavailable" || !strings.Contains(record.Error, "already in use") {
		t.Fatalf("record=%+v", record)
	}
}

func TestFTSProfileByteLimit(t *testing.T) {
	writer := &ftsProfileWriter{writer: io.Discard, bytes: pprofMaxCaptureBytes - 1}
	if n, err := writer.Write([]byte("ab")); n != 0 || err == nil || writer.err == nil {
		t.Fatalf("write=%d,%v", n, err)
	}
}
