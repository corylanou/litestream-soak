package worker

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func shortRestoredCompareRetry(t *testing.T) {
	t.Helper()
	timeout, attempts, delay := logicalReadBusyTimeout, restoredCompareAttempts, restoredCompareRetryDelay
	logicalReadBusyTimeout, restoredCompareAttempts, restoredCompareRetryDelay = 20*time.Millisecond, 3, 10*time.Millisecond
	t.Cleanup(func() {
		logicalReadBusyTimeout, restoredCompareAttempts, restoredCompareRetryDelay = timeout, attempts, delay
	})
}

func holdExclusiveLock(t *testing.T, path string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			_ = conn.Close()
			_ = db.Close()
		})
	}
	t.Cleanup(release)
	return release
}

func TestCompareRestoredLogicalLock(t *testing.T) {
	shortRestoredCompareRetry(t)
	tests := []struct {
		name     string
		restored string
		lock     bool
		release  time.Duration
		locked   bool
		want     string
	}{
		{name: "match", restored: "INSERT INTO t VALUES (1), (2)"},
		{name: "busy for the whole comparison", restored: "INSERT INTO t VALUES (1), (2)", lock: true, locked: true, want: "restored comparison: database is locked"},
		{name: "busy then released within the retries", restored: "INSERT INTO t VALUES (1), (2)", lock: true, release: 25 * time.Millisecond},
		{name: "content mismatch", restored: "INSERT INTO t VALUES (1), (3)", want: `logical table "t" mismatch`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			sourcePath, restoredPath := filepath.Join(dir, "source.db"), filepath.Join(dir, "restored.db")
			logicalTestDB(t, sourcePath, "CREATE TABLE t(x); INSERT INTO t VALUES (1), (2)")
			logicalTestDB(t, restoredPath, "CREATE TABLE t(x); "+tt.restored)
			v := NewVerifier(DefaultConfig())
			source, err := v.logicalValidation(context.Background(), sourcePath, 1)
			if err != nil {
				t.Fatal(err)
			}
			if tt.lock {
				release := holdExclusiveLock(t, restoredPath)
				if tt.release > 0 {
					time.AfterFunc(tt.release, release)
				}
			}
			err = v.compareRestoredLogical(context.Background(), source, restoredPath)
			var locked *restoredComparisonLockedError
			if got := errors.As(err, &locked); got != tt.locked {
				t.Fatalf("locked = %v, want %v (err %v)", got, tt.locked, err)
			}
			if tt.want == "" {
				if err != nil {
					t.Fatalf("compareRestoredLogical() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("compareRestoredLogical() error = %v, want %q", err, tt.want)
			}
			if got := reporting.RestoredComparisonLocked(err.Error()); got != tt.locked {
				t.Fatalf("RestoredComparisonLocked(%q) = %v, want %v", err.Error(), got, tt.locked)
			}
		})
	}
}

func TestFollowCheckComparisonLockIsInconclusive(t *testing.T) {
	shortRestoredCompareRetry(t)
	tests := []struct {
		name      string
		lock      bool
		sourceSQL string
		status    string
		want      string
	}{
		{name: "follow copy locked during comparison", lock: true, status: "aborted", want: "follow restore comparison inconclusive at txid 0000000000000043"},
		{name: "follow copy diverged", sourceSQL: "INSERT INTO t VALUES (3)", status: "failed", want: "follow restore diverged from source at txid 0000000000000043"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newFollowFixture(t)
			if result := fixture.cycle(t, 0x42, 100); result.Status != "pending" {
				t.Fatalf("first cycle = %+v", result)
			}
			if tt.sourceSQL != "" {
				logicalTestDB(t, fixture.cfg.DBPath, tt.sourceSQL)
			}
			if tt.lock {
				holdExclusiveLock(t, followDBPath(fixture.cfg.DBPath))
			}
			result := fixture.cycle(t, 0x43, 200)
			if result.Status != tt.status || result.Passed || !strings.Contains(result.ErrorMessage, tt.want) {
				t.Fatalf("status %q error %q, want status %q error containing %q", result.Status, result.ErrorMessage, tt.status, tt.want)
			}
			if tt.lock {
				if strings.Contains(result.ErrorMessage, "diverged") || !strings.HasPrefix(result.Summary, "verification inconclusive:") {
					t.Fatalf("locked comparison reported as %q / %q", result.Summary, result.ErrorMessage)
				}
				classification := result.InconclusiveClassification
				if classification == nil || classification.Stage != reporting.StageHarnessLock || classification.Signature != reporting.SignatureRestoredComparisonLocked || !classification.Valid() {
					t.Fatalf("inconclusive classification = %+v", classification)
				}
				if fixture.v.follow == nil {
					t.Fatal("a locked comparison stopped the follow session")
				}
				return
			}
			if result.InconclusiveClassification != nil || fixture.v.follow != nil {
				t.Fatalf("divergence classification %+v follow running %v", result.InconclusiveClassification, fixture.v.follow != nil)
			}
		})
	}
}
