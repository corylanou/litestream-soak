package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteFileAtomic(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		body     string
		breakDir bool
		wantErr  bool
		wantBody string
	}{
		{name: "creates a new file", body: "dbs: []\n", wantBody: "dbs: []\n"},
		{name: "replaces an existing file whole", existing: "old\n", body: "new\n", wantBody: "new\n"},
		{name: "a failed commit keeps the previous file", existing: "old\n", body: "new\n", breakDir: true, wantErr: true, wantBody: "old\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.breakDir && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions")
			}
			dir := t.TempDir()
			path := filepath.Join(dir, "litestream.yml")
			if tt.existing != "" {
				if err := os.WriteFile(path, []byte(tt.existing), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if tt.breakDir {
				if err := os.Chmod(dir, 0o500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
			}

			err := writeFileAtomic(path, []byte(tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("writeFileAtomic() error = %v, wantErr %v", err, tt.wantErr)
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatalf("read %s: %v", path, readErr)
			}
			if string(body) != tt.wantBody {
				t.Fatalf("body = %q, want %q", body, tt.wantBody)
			}
			if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
				t.Fatalf("temp file left behind: %v", statErr)
			}
		})
	}
}

func TestWriteLitestreamConfigNeverLeavesTruncatedConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.DBPath = filepath.Join(dir, "test.db")
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.ReplicaType = "file"
	cfg.ReplicaPath = filepath.Join(dir, "replica")

	runner := NewRunner(cfg)
	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("writeLitestreamConfig() error = %v", err)
	}
	first, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first), "dbs:") {
		t.Fatalf("config missing dbs section:\n%s", first)
	}

	if err := runner.writeLitestreamConfig(); err != nil {
		t.Fatalf("second writeLitestreamConfig() error = %v", err)
	}
	second, err := os.ReadFile(cfg.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != string(first) {
		t.Fatalf("rewritten config differs:\n%s\n---\n%s", first, second)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temp file %s left in data dir", entry.Name())
		}
	}
}
