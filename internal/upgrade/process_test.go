package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("UPGRADE_HELPER") == "1" && len(os.Args) > 1 && (os.Args[1] == "replicate" || os.Args[1] == "restore") {
		if err := helperProcess(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func helperProcess() error {
	data, err := os.ReadFile(os.Args[3])
	if err != nil {
		return err
	}
	paths := regexp.MustCompile(`path: "([^"]+)"`).FindAllStringSubmatch(string(data), -1)
	if len(paths) != 2 {
		return fmt.Errorf("expected isolated database and replica paths")
	}
	source, replica := paths[0][1], paths[1][1]
	if filepath.Dir(source) != filepath.Dir(replica) {
		return fmt.Errorf("replica is outside arm")
	}
	if os.Args[1] == "restore" {
		if os.Getenv("UPGRADE_RESTORE_WARN") == "1" {
			fmt.Println(`level=WARN msg="retry succeeded"`)
		}
		if os.Getenv("UPGRADE_FAIL_PRE") == "1" && strings.Contains(os.Args[5], "pre-candidate") {
			return fmt.Errorf("known-bad pre-upgrade restore")
		}
		data, err := os.ReadFile(filepath.Join(replica, "backup"))
		if err != nil {
			return err
		}
		return os.WriteFile(os.Args[5], data, 0600)
	}
	if os.Getenv("UPGRADE_SLOW_START") == "1" {
		time.Sleep(800 * time.Millisecond)
	}
	fmt.Println("binary=" + os.Args[0])
	if os.Getenv("UPGRADE_WARN") == "1" {
		fmt.Println(`level=WARN msg="retry recovered after transient failure"`)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer signal.Stop(signals)
	fmt.Println(`msg="replicating to"`)
	if os.Getenv("UPGRADE_NO_MAINT") != "1" {
		fmt.Println(`msg="snapshot complete"`)
		fmt.Println(`msg="snapshot complete"`)
		fmt.Println(`msg="compaction complete"`)
		fmt.Println(`msg="compaction complete"`)
		fmt.Println(`msg="l0 retention enforced" deleted_count=2`)
	}
	<-signals
	if err := os.MkdirAll(replica, 0700); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		return err
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	data, err = os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(replica, "backup"), data, 0600)
}

func helperConfig(t *testing.T) Config {
	t.Helper()
	t.Setenv("UPGRADE_HELPER", "1")
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fileHash(path)
	if err != nil {
		t.Fatal(err)
	}
	return Config{Mode: "persistent-upgrade", Output: filepath.Join(t.TempDir(), "run"), Baseline: Binary{Path: path, SHA256: hash}, Candidate: Binary{Path: path, SHA256: hash}, Transition: "ltx-v1", Rollback: true, AgeTimeout: 5 * time.Second, ContinueFor: 200 * time.Millisecond}
}

func TestLifecycleRetainsFailureThroughSuccessfulRecovery(t *testing.T) {
	cfg := helperConfig(t)
	t.Setenv("UPGRADE_FAIL_PRE", "1")
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "failed" {
		t.Fatalf("result = %+v", result)
	}
	for _, check := range result.Checks {
		expected := "passed"
		if check.Name == "pre-candidate" {
			expected = "failed"
		}
		if check.Status != expected {
			t.Fatalf("check = %+v", check)
		}
	}
	for _, name := range []string{"result.json", "fixture.json", "fixture/db", "candidate/replica/backup", "pre-candidate.log", "post-candidate.db", "rollback/replica/backup"} {
		if _, err := os.Stat(filepath.Join(cfg.Output, name)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLifecycleFreshAndReusedFixture(t *testing.T) {
	cfg := helperConfig(t)
	result, err := Run(context.Background(), cfg)
	if err != nil || result.Status != "passed" {
		t.Fatalf("%+v %v", result, err)
	}
	cfg.Fixture = filepath.Join(cfg.Output, "fixture")
	cfg.Output = filepath.Join(t.TempDir(), "reuse")
	result, err = Run(context.Background(), cfg)
	if err != nil || result.Status != "passed" {
		t.Fatalf("%+v %v", result, err)
	}
	cfg.Fixture = ""
	cfg.Mode = "fresh-start"
	cfg.Output = filepath.Join(t.TempDir(), "fresh")
	cfg.Rollback = false
	result, err = Run(context.Background(), cfg)
	if err != nil || result.Status != "inconclusive" {
		t.Fatalf("%+v %v", result, err)
	}
	if result.Checks[len(result.Checks)-1].Status != "unsupported" {
		t.Fatal(result.Checks)
	}
}

func TestUnsupportedTransitionDoesNotExecute(t *testing.T) {
	cfg := helperConfig(t)
	cfg.Transition = "v0.3-to-v0.5"
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "inconclusive" {
		t.Fatal(result)
	}
	for _, check := range result.Checks {
		if check.Status != "unsupported" {
			t.Fatal(check)
		}
	}
	if _, err := os.Stat(filepath.Join(cfg.Output, "fixture")); !os.IsNotExist(err) {
		t.Fatal("executed unsupported transition")
	}
}

func TestLogicalComparisonDetectsChangedAndMissingRows(t *testing.T) {
	for _, statement := range []string{"UPDATE t SET value='corrupt' WHERE id=1", "DELETE FROM t WHERE id=2", "UPDATE t SET value=x'73656564' WHERE id=1", "CREATE INDEX extra ON t(value)", "PRAGMA user_version=7", "PRAGMA application_id=9"} {
		t.Run(statement, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			copy := filepath.Join(root, "copy")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			if err := seed(source); err != nil {
				t.Fatal(err)
			}
			if err := copyState(source, copy); err != nil {
				t.Fatal(err)
			}
			if err := compare(filepath.Join(source, "db"), filepath.Join(copy, "db")); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", filepath.Join(copy, "db"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := compare(filepath.Join(source, "db"), filepath.Join(copy, "db")); err == nil {
				t.Fatal("accepted corrupt restore")
			}
		})
	}
}

func TestInsufficientAgeIsNotSuccess(t *testing.T) {
	cfg := helperConfig(t)
	t.Setenv("UPGRADE_NO_MAINT", "1")
	cfg.AgeTimeout = 250 * time.Millisecond
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "inconclusive" || result.Checks[0].Status != "inconclusive" {
		t.Fatal(result)
	}
	if result.Checks[1].Status != "unexecuted" {
		t.Fatal("continued unaged fixture")
	}
}

func TestCancelledRunRetainsResult(t *testing.T) {
	cfg := helperConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Run(ctx, cfg)
	if err == nil || result.Status != "failed" {
		t.Fatalf("%+v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Output, "result.json")); err != nil {
		t.Fatal(err)
	}
}

func TestKnownBadFixtureCannotBecomeCleanOnReuse(t *testing.T) {
	cfg := helperConfig(t)
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(cfg.Output, "fixture")
	if err := sealFixture(fixture, result.Age, cfg.Baseline.SHA256, "earlier corruption"); err != nil {
		t.Fatal(err)
	}
	cfg.Fixture = fixture
	cfg.Output = filepath.Join(t.TempDir(), "reuse")
	result, err = Run(context.Background(), cfg)
	if err == nil || result.Status != "failed" || !strings.Contains(err.Error(), "earlier corruption") {
		t.Fatalf("%+v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Output, "fixture", "db")); err != nil {
		t.Fatal("failed fixture was not preserved")
	}
}

func TestFreshCandidateStartsWithCandidateBinary(t *testing.T) {
	cfg := helperConfig(t)
	cfg.Mode = "fresh-start"
	data, err := os.ReadFile(cfg.Baseline.Path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Candidate.Path = filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(cfg.Candidate.Path, data, 0700); err != nil {
		t.Fatal(err)
	}
	result, err := Run(context.Background(), cfg)
	if err != nil || result.Status != "passed" {
		t.Fatalf("%+v %v", result, err)
	}
	data, err = os.ReadFile(filepath.Join(cfg.Output, "candidate-fresh.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "binary="+cfg.Candidate.Path) {
		t.Fatalf("candidate fresh arm ran wrong binary: %s", data)
	}
}

func TestWarningRecoveryCannotPass(t *testing.T) {
	cfg := helperConfig(t)
	t.Setenv("UPGRADE_WARN", "1")
	result, err := Run(context.Background(), cfg)
	if err == nil || result.Status != "failed" {
		t.Fatalf("warning recovery passed: %+v %v", result, err)
	}
}

func TestContinuationWaitsForReplicatorReadiness(t *testing.T) {
	cfg := helperConfig(t)
	t.Setenv("UPGRADE_SLOW_START", "1")
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := seed(state); err != nil {
		t.Fatal(err)
	}
	_, err := exercise(context.Background(), cfg.Baseline, state, state+".log", time.Second, false, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRetryCannotPass(t *testing.T) {
	cfg := helperConfig(t)
	t.Setenv("UPGRADE_RESTORE_WARN", "1")
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "failed" {
		t.Fatalf("restore retry passed: %+v", result)
	}
}

func TestChangedBinaryCannotExecuteLaterPhase(t *testing.T) {
	cfg := helperConfig(t)
	cfg.Baseline.SHA256 = strings.Repeat("0", 64)
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := seed(state); err != nil {
		t.Fatal(err)
	}
	_, err := exercise(context.Background(), cfg.Baseline, state, state+".log", time.Second, false, 200*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "pin") {
		t.Fatalf("changed binary executed: %v", err)
	}
}
