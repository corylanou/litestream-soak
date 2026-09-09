package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestFTSPinnedRestoreComparison(t *testing.T) {
	if os.Getenv("SOAK_FTS_BASELINE_BINARY") == "" && os.Getenv("SOAK_FTS_CANDIDATE_BINARY") == "" {
		t.Skip("opt-in: set SOAK_FTS_{BASELINE,CANDIDATE}_{BINARY,SHA} and SOAK_FTS_EVIDENCE_DIR for immutable comparison")
	}
	root := os.Getenv("SOAK_FTS_EVIDENCE_DIR")
	if root == "" {
		t.Fatal("SOAK_FTS_EVIDENCE_DIR is required to retain all comparison evidence")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("SOAK_FTS_EVIDENCE_DIR must be absolute")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"BASELINE", "CANDIDATE"} {
		t.Run(strings.ToLower(role), func(t *testing.T) {
			binary := os.Getenv("SOAK_FTS_" + role + "_BINARY")
			sha := os.Getenv("SOAK_FTS_" + role + "_SHA")
			if !filepath.IsAbs(binary) || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(sha) {
				t.Fatal("explicit absolute binary path and immutable 40-character SHA are required")
			}
			version, err := exec.Command(binary, "version").CombinedOutput()
			if err != nil || !strings.Contains(string(version), sha) {
				t.Fatalf("binary identity mismatch: version=%q expected=%s error=%v", version, sha, err)
			}
			dir, err := os.MkdirTemp(root, strings.ToLower(role)+"-")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("role=%s candidate=%s evidence=%s", role, sha, dir)
			cfg := DefaultConfig()
			cfg.DataDir = dir
			cfg.DBPath = filepath.Join(dir, "fts.db")
			cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
			cfg.ReplicaPath = filepath.Join(dir, "replica")
			cfg.SocketPath = filepath.Join(os.TempDir(), fmt.Sprintf("fts-%d.sock", time.Now().UnixNano()))
			cfg.LoadMode = "fts"
			cfg.ProfileName = "fts-maintenance"
			cfg.LitestreamSHA = sha
			cfg.RunID = filepath.Base(dir)
			cfg.WorkerID = cfg.RunID
			cfg.GitSHA = os.Getenv("SOAK_FTS_WORKER_SHA")
			cfg.WorkloadSHA = cfg.GitSHA
			config := fmt.Sprintf("socket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    min-checkpoint-page-count: 1\n    checkpoint-interval: 100ms\n    replicas:\n      - path: %q\n        sync-interval: 100ms\n", cfg.SocketPath, cfg.DBPath, cfg.ReplicaPath)
			if err := os.WriteFile(cfg.ConfigPath, []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			db, err := openFTS(ctx, cfg.DBPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			log, err := os.OpenFile(filepath.Join(dir, "replicate.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, binary, "replicate", "-config", cfg.ConfigPath)
			cmd.Stdout = log
			cmd.Stderr = log
			if err := cmd.Start(); err != nil {
				_ = log.Close()
				t.Fatal(err)
			}
			defer func() { cancel(); _ = cmd.Wait(); _ = log.Close() }()
			evidence, err := os.OpenFile(filepath.Join(dir, "results.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = evidence.Close() }()
			encode := func(value any) {
				t.Helper()
				if err := json.NewEncoder(evidence).Encode(value); err != nil {
					t.Fatal(err)
				}
			}
			encode(map[string]string{"role": role, "candidate_sha": sha, "version": strings.TrimSpace(string(version)), "workload": "fts-v1", "worker_sha": cfg.GitSHA})
			v := NewVerifier(cfg)
			var synced syncResponse
			if !waitUntil(30*time.Second, 50*time.Millisecond, func() bool {
				synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
				return err == nil && synced.TXID > 0 && synced.ReplicatedTXID >= synced.TXID
			}) {
				encode(map[string]string{"error": fmt.Sprint(err)})
				t.Fatalf("initial sync: %v", err)
			}
			profiles := newPprofCapturer(&cfg)
			profiles.identityOnce.Do(func() {
				profiles.binaries = map[string]profileBinary{"litestream": readProfileBinary(binary)}
			})
			totals := map[string]int64{}
			for step := int64(0); step < 8; step++ {
				phase := ftsPhase(step)
				profiles.captureSet(ctx, "fts-"+phase+"-before")
				finish := profiles.beginFTSProfile(ctx, phase)
				work, err := stepFTS(ctx, db)
				finish()
				encode(map[string]any{"step": step, "work": work, "error": fmt.Sprint(err)})
				if err != nil {
					t.Fatal(err)
				}
				totals[phase] += work.Changes
				profiles.captureSet(ctx, "fts-"+phase+"-after")
				if err := v.waitForSync(ctx, nil); err != nil {
					encode(map[string]string{"sync_error": err.Error()})
					t.Fatal(err)
				}
				synced, err = v.syncOnceDB(ctx, time.Second, cfg.DBPath)
				if err != nil {
					t.Fatal(err)
				}
				source, err := readLogicalSnapshot(ctx, cfg.DBPath, cfg.logicalLimits())
				if err != nil {
					t.Fatal(err)
				}
				restored := filepath.Join(dir, fmt.Sprintf("restored-%d.db", step))
				output, restoreErr := exec.CommandContext(ctx, binary, "restore", "-config", cfg.ConfigPath, "-txid", formatTXID(synced.TXID), "-o", restored, cfg.DBPath).CombinedOutput()
				encode(map[string]any{"step": step, "txid": formatTXID(synced.TXID), "restore_output": string(output), "restore_error": fmt.Sprint(restoreErr)})
				if restoreErr != nil {
					t.Fatalf("pinned restore: %s: %v", output, restoreErr)
				}
				actual, err := readLogicalSnapshot(ctx, restored, cfg.logicalLimits())
				if err != nil {
					t.Fatal(err)
				}
				if err := compareLogicalSnapshots(source, actual); err != nil {
					t.Fatal(err)
				}
				encode(map[string]any{"step": step, "logical_match": true, "search_match": true})
				if step == 7 {
					restoredDB := logicalTestDB(t, restored, "DELETE FROM fts_documents WHERE id=1")
					if _, err := readLogicalSnapshot(ctx, restored, cfg.logicalLimits()); err == nil {
						t.Fatal("negative restore passed")
					} else {
						encode(map[string]string{"expected_negative_error": err.Error()})
					}
					if err := restoredDB.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, phase := range []string{"insert", "update", "delete", "query", "merge", "optimize"} {
				if totals[phase] <= 0 {
					t.Fatalf("no actual %s work: %v", phase, totals)
				}
			}
			records, err := filepath.Glob(filepath.Join(dir, "profiles", "*.pprof.json"))
			if err != nil {
				t.Fatal(err)
			}
			available := map[string]bool{}
			for _, filename := range records {
				body, err := os.ReadFile(filename)
				if err != nil {
					t.Fatal(err)
				}
				var record profileRecord
				if err := json.Unmarshal(body, &record); err != nil {
					t.Fatal(err)
				}
				if record.Status == "available" {
					available[record.Phase+":"+record.Kind] = true
				}
			}
			for _, phase := range []string{"insert", "update", "delete", "query", "merge", "optimize"} {
				for _, suffix := range []string{"-during:worker_cpu", "-before:heap", "-after:heap"} {
					if !available["fts-"+phase+suffix] {
						t.Errorf("profile unavailable for %s%s", phase, suffix)
					}
				}
			}
			encode(map[string]any{"totals": totals, "profiles": available, "passed": !t.Failed()})
		})
	}
}
