package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type fakeReplicaStore struct {
	keys    []string
	deleted []string
	listErr error
}

func (s *fakeReplicaStore) purger() replicaL9Purger {
	return replicaL9Purger{
		list: func(context.Context, Config) ([]string, error) {
			if s.listErr != nil {
				return nil, s.listErr
			}
			return slices.Clone(s.keys), nil
		},
		remove: func(_ context.Context, _ Config, key string) error {
			s.deleted = append(s.deleted, key)
			s.keys = slices.DeleteFunc(s.keys, func(k string) bool { return k == key })
			return nil
		},
	}
}

func s3LaneConfig() Config {
	cfg := DefaultConfig()
	cfg.ReplicaType = "s3"
	cfg.S3Bucket = "bucket"
	cfg.S3Path = "soak/up-nol9-a25ac011/worker-up-nol9-a25ac011-gate-upgrade-lane-nol9/vol-1"
	return cfg
}

func TestPurgeReplicaL9(t *testing.T) {
	own := "soak/up-nol9-a25ac011/worker-up-nol9-a25ac011-gate-upgrade-lane-nol9/vol-1/"
	tests := []struct {
		name          string
		keys          []string
		listErr       error
		wantDeleted   []string
		wantErr       error
		wantErrText   string
		wantRemaining int
	}{
		{
			name: "deletes only L9 objects under the prefix",
			keys: []string{
				own + "0000/0000000000000001-0000000000000001.ltx",
				own + "0001/0000000000000001-0000000000000040.ltx",
				own + "0003/0000000000000001-0000000000000030.ltx",
				own + "0009/0000000000000001-0000000000000001.ltx",
				own + "0009/0000000000000001-0000000000000020.ltx",
				own + "pprof/0009-not-a-level.pb.gz",
			},
			wantDeleted: []string{
				own + "0009/0000000000000001-0000000000000001.ltx",
				own + "0009/0000000000000001-0000000000000020.ltx",
			},
		},
		{
			name:        "nothing to delete is not an error",
			keys:        []string{own + "0000/0000000000000001-0000000000000001.ltx"},
			wantDeleted: []string{},
		},
		{
			name: "an object outside the prefix aborts before any delete",
			keys: []string{
				own + "0009/0000000000000001-0000000000000001.ltx",
				"soak/up-a25ac011/worker-up-a25ac011-gate-upgrade-lane/vol-9/0009/0000000000000001-0000000000000001.ltx",
			},
			wantDeleted: []string{},
			wantErr:     errReplicaL9Outside,
		},
		{
			name:        "path traversal in a key aborts",
			keys:        []string{own + "0009/../../other/0009/0000000000000001-0000000000000001.ltx"},
			wantDeleted: []string{},
			wantErr:     errReplicaL9Outside,
		},
		{
			name:        "listing failure deletes nothing",
			listErr:     errors.New("s3cmd: access denied"),
			wantDeleted: []string{},
			wantErrText: "list replica objects",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeReplicaStore{keys: slices.Clone(tt.keys), listErr: tt.listErr}
			cfg := s3LaneConfig()
			now := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
			record, err := store.purger().purgeReplicaL9(context.Background(), cfg, func() time.Time { return now })
			switch {
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			case tt.wantErrText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrText)):
				t.Fatalf("error = %v, want %q", err, tt.wantErrText)
			case tt.wantErr == nil && tt.wantErrText == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(store.deleted, tt.wantDeleted) {
				t.Fatalf("deleted = %v, want %v", store.deleted, tt.wantDeleted)
			}
			if !slices.Equal(record.Deleted, tt.wantDeleted) || record.Prefix != strings.TrimSuffix(own, "/") || record.CompletedAt.IsZero() {
				t.Fatalf("record = %+v", record)
			}
			if err == nil && (record.RemainingL9 != 0 || record.Listed != len(tt.keys)) {
				t.Fatalf("record = %+v", record)
			}
			if err != nil && record.Error == "" {
				t.Fatalf("record did not keep the error: %+v", record)
			}
			for _, key := range store.keys {
				if level, ok := replicaLevelOfKey(key); ok && level == replicaL9Level && err == nil {
					t.Fatalf("L9 object survived the purge: %s", key)
				}
			}
		})
	}
}

func TestPurgeReplicaL9FailsWhenObjectsRemain(t *testing.T) {
	own := s3LaneConfig().S3Path + "/"
	key := own + "0009/0000000000000001-0000000000000001.ltx"
	purger := replicaL9Purger{
		list:   func(context.Context, Config) ([]string, error) { return []string{key}, nil },
		remove: func(context.Context, Config, string) error { return nil },
	}
	record, err := purger.purgeReplicaL9(context.Background(), s3LaneConfig(), time.Now)
	if err == nil || !strings.Contains(err.Error(), "1 L9 object(s) remain") || record.RemainingL9 != 1 {
		t.Fatalf("purge = %+v, %v", record, err)
	}
}

func TestPurgeReplicaL9FileBackend(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.ReplicaType = "file"
	cfg.ReplicaPath = filepath.Join(dir, "replicas", "lane")
	files := map[string]bool{
		"0000/0000000000000001-0000000000000001.ltx": false,
		"0001/0000000000000001-0000000000000040.ltx": false,
		"0009/0000000000000001-0000000000000001.ltx": true,
		"0009/0000000000000001-0000000000000030.ltx": true,
	}
	for name := range files {
		path := filepath.Join(cfg.ReplicaPath, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("ltx"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	neighbour := filepath.Join(dir, "replicas", "other", "0009", "0000000000000001-0000000000000001.ltx")
	if err := os.MkdirAll(filepath.Dir(neighbour), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(neighbour, []byte("ltx"), 0o644); err != nil {
		t.Fatal(err)
	}
	record, err := newReplicaL9Purger().purgeReplicaL9(context.Background(), cfg, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Deleted) != 2 || record.Listed != 4 || record.RemainingL9 != 0 {
		t.Fatalf("record = %+v", record)
	}
	for name, deleted := range files {
		_, err := os.Stat(filepath.Join(cfg.ReplicaPath, filepath.FromSlash(name)))
		if deleted && !os.IsNotExist(err) {
			t.Fatalf("%s still present: %v", name, err)
		}
		if !deleted && err != nil {
			t.Fatalf("%s removed: %v", name, err)
		}
	}
	if _, err := os.Stat(neighbour); err != nil {
		t.Fatalf("object outside the lane prefix touched: %v", err)
	}
}

func TestParseS3ListingKeys(t *testing.T) {
	listing := strings.NewReader(`2026-10-29 12:00         4096  s3://bucket/soak/lane/0000/0000000000000001-0000000000000001.ltx
                          DIR  s3://bucket/soak/lane/0009/
2026-10-29 12:01         8192  s3://bucket/soak/lane/0009/0000000000000001-0000000000000020.ltx
2026-10-29 12:02           12  s3://bucket/soak/lane/notes/with space.txt
garbage line
2026-10-29 12:03           12  s3://other/soak/lane/0009/0000000000000001-0000000000000020.ltx
`)
	keys, err := parseS3ListingKeys(listing, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"soak/lane/0000/0000000000000001-0000000000000001.ltx",
		"soak/lane/0009/0000000000000001-0000000000000020.ltx",
		"soak/lane/notes/with space.txt",
	}
	if !slices.Equal(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
}

func TestReplicaKeyWithinPrefix(t *testing.T) {
	cfg := s3LaneConfig()
	for key, want := range map[string]bool{
		cfg.S3Path + "/0009/a.ltx":         true,
		cfg.S3Path + "-other/0009/a.ltx":   false,
		"soak/up-a25ac011/x/0009/a.ltx":    false,
		cfg.S3Path + "/0009/../../x/a.ltx": false,
		cfg.S3Path:                         false,
	} {
		if got := replicaKeyWithinPrefix(cfg, key); got != want {
			t.Fatalf("replicaKeyWithinPrefix(%q) = %v, want %v", key, got, want)
		}
	}
	empty := s3LaneConfig()
	empty.S3Path = "/"
	noop := replicaL9Purger{list: func(context.Context, Config) ([]string, error) { return nil, nil }}
	if _, err := noop.purgeReplicaL9(context.Background(), empty, time.Now); !errors.Is(err, errReplicaL9Outside) {
		t.Fatalf("empty prefix err = %v", err)
	}
}
