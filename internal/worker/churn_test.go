package worker

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/churn"
)

func TestChurnConfiguration(t *testing.T) {
	t.Setenv("LOAD_MODE", "queue")
	t.Setenv("CHURN_CONFIG", `{"slots":16,"hot_percent":80,"workers":2,"rate":100,"payload_size":32,"seed":7}`)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Churn.Slots != 16 || cfg.WorkloadConfig().Churn.Seed != 7 {
		t.Fatalf("churn config lost: %+v", cfg.Churn)
	}
	t.Setenv("CHURN_CONFIG", `{"slots":-1}`)
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("invalid churn config accepted")
	}
}

func TestChurnRestoredInvariants(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restore.db")
	db, err := churn.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("INSERT INTO churn_jobs VALUES(0,'done',1,'payload')"); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.Churn = churn.Config{Slots: 4}
	snapshot, err := readLogicalSnapshot(ctx, path, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	v := NewVerifier(cfg)
	if err := v.compareRestoredLogical(ctx, snapshot, path); err == nil {
		t.Fatal("identically corrupt source and restore accepted")
	}
	if _, err := db.Exec("INSERT INTO churn_receipts VALUES(0)"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = readLogicalSnapshot(ctx, path, cfg.logicalLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := v.compareRestoredLogical(ctx, snapshot, path); err != nil {
		t.Fatal(err)
	}
}

func TestChurnMetricsRetainBusyError(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.LoadMode = "queue"
	cfg.WorkerID = t.Name()
	db, err := churn.Open(ctx, filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("INSERT INTO churn_jobs VALUES(0,'ready',0,'x')"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	n, err := churn.Apply(ctx, db, "queue", "enqueue", 1, 0, 8)
	if err == nil {
		t.Fatal("expected busy error")
	}
	recordChurn(cfg, "enqueue", n, time.Since(started), err)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	n, err = churn.Apply(ctx, db, "queue", "enqueue", 1, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	recordChurn(cfg, "enqueue", n, time.Millisecond, nil)
	labels := []string{"queue", "enqueue", cfg.WorkerID, cfg.ProfileName, cfg.Source}
	if got := testutil.ToFloat64(churnMutations.WithLabelValues(labels...)); got != 1 {
		t.Fatalf("mutations=%v", got)
	}
	if got := testutil.ToFloat64(churnErrors.WithLabelValues("queue", "enqueue", "busy", cfg.WorkerID, cfg.ProfileName, cfg.Source)); got != 1 {
		t.Fatalf("retained busy errors=%v", got)
	}
}

func TestChurnLoadLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.LoadMode = "cache"
	cfg.DBPath = filepath.Join(t.TempDir(), "db")
	cfg.Churn = churn.Config{Slots: 4, Workers: 2, Rate: 100, PayloadSize: 8}
	r := NewRunner(cfg)
	if err := r.populate(ctx); err != nil {
		t.Fatal(err)
	}
	load, err := startChurn(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := load.engine.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	load.Stop()
	if err := validateChurnRestore(ctx, cfg, cfg.DBPath); err != nil {
		t.Fatal(err)
	}
}

func TestChurnProvenanceUsesSuppliedEnvironment(t *testing.T) {
	t.Setenv("CHURN_CONFIG", `{"slots":99}`)
	cfg, err := WorkloadFromEnvironment(map[string]string{"LOAD_MODE": "queue", "CHURN_CONFIG": `{"slots":16,"seed":7}`})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Churn.Slots != 16 || cfg.Churn.Seed != 7 {
		t.Fatalf("provenance used ambient environment: %+v", cfg.Churn)
	}
}
