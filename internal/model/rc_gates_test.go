package model

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openRCGateTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func testRCGate(started time.Time) RCGate {
	return RCGate{
		Repository:      "benbjohnson/litestream",
		CandidateRef:    "v0.5.0-rc1",
		CandidateSource: "rc-aaaaaaaa",
		CandidateSHA:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaselineSource:  "rcbase-bbbbbbbb",
		BaselineSHA:     "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Duration:        72 * time.Hour,
		Tail:            true,
		Status:          RCGateRunning,
		StartedAt:       started,
		ExpiresAt:       started.Add(72 * time.Hour),
		TailExpiresAt:   new(started.Add(7 * 24 * time.Hour)),
		Resources: []RCGateResource{{
			Source:     "rc-aaaaaaaa",
			WorkerName: "worker-rc-aaaaaaaa-gate-stress",
			Profile:    "gate-stress",
			Region:     "ord",
			CPUs:       2,
			MemoryMB:   2048,
			VolumeGB:   10,
			StartedAt:  started,
			ExpiresAt:  started.Add(7 * 24 * time.Hour),
		}},
	}
}

func TestRCGateLedgerRoundTrip(t *testing.T) {
	db := openRCGateTestDB(t)
	started := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	gate := testRCGate(started)
	if err := db.CreateRCGate(&gate); err != nil {
		t.Fatalf("CreateRCGate() error = %v", err)
	}
	if gate.ID == 0 {
		t.Fatal("CreateRCGate() did not assign an ID")
	}

	gates, err := db.ListRCGates()
	if err != nil {
		t.Fatalf("ListRCGates() error = %v", err)
	}
	if len(gates) != 1 {
		t.Fatalf("gates = %d, want 1", len(gates))
	}
	got := gates[0]
	if got.CandidateSHA != gate.CandidateSHA || got.BaselineSHA != gate.BaselineSHA || got.Duration != 72*time.Hour || !got.Tail {
		t.Fatalf("gate = %+v", got)
	}
	if !got.StartedAt.Equal(started) || !got.ExpiresAt.Equal(gate.ExpiresAt) || got.TailExpiresAt == nil || !got.TailExpiresAt.Equal(*gate.TailExpiresAt) {
		t.Fatalf("gate times = %+v", got)
	}
	if len(got.Resources) != 1 || got.Resources[0].VolumeGB != 10 || got.Resources[0].MemoryMB != 2048 {
		t.Fatalf("resources = %+v", got.Resources)
	}

	active, err := db.ActiveRCGateForSource("rcbase-bbbbbbbb")
	if err != nil || active == nil || active.ID != gate.ID {
		t.Fatalf("ActiveRCGateForSource() = %+v, %v", active, err)
	}
	none, err := db.ActiveRCGateForSource("pr-12")
	if err != nil || none != nil {
		t.Fatalf("ActiveRCGateForSource(pr-12) = %+v, %v", none, err)
	}
}

func TestRCGateRejectsOverlappingActiveSource(t *testing.T) {
	db := openRCGateTestDB(t)
	started := time.Now().UTC()
	first := testRCGate(started)
	if err := db.CreateRCGate(&first); err != nil {
		t.Fatalf("CreateRCGate() error = %v", err)
	}
	second := testRCGate(started)
	second.CandidateSource = "rc-cccccccc"
	second.CandidateSHA = "cccccccccccccccccccccccccccccccccccccccc"
	if err := db.CreateRCGate(&second); !errors.Is(err, ErrRCGateSourceActive) {
		t.Fatalf("overlapping CreateRCGate() error = %v, want ErrRCGateSourceActive", err)
	}

	if err := db.UpdateRCGateStatus(first.ID, RCGateCompleted, new(started)); err != nil {
		t.Fatalf("UpdateRCGateStatus() error = %v", err)
	}
	if err := db.CreateRCGate(&second); err != nil {
		t.Fatalf("CreateRCGate() after completion error = %v", err)
	}
	active, err := db.ActiveRCGates()
	if err != nil {
		t.Fatalf("ActiveRCGates() error = %v", err)
	}
	if len(active) != 1 || active[0].ID != second.ID {
		t.Fatalf("active = %+v, want only second gate", active)
	}
}

func TestRCGatePinImageIsWriteOnce(t *testing.T) {
	db := openRCGateTestDB(t)
	gate := testRCGate(time.Now().UTC())
	if err := db.CreateRCGate(&gate); err != nil {
		t.Fatalf("CreateRCGate() error = %v", err)
	}
	pinned, err := db.PinRCGateImage(gate.ID, gate.CandidateSource, "registry.fly.io/litestream-soak:first")
	if err != nil || pinned != "registry.fly.io/litestream-soak:first" {
		t.Fatalf("first pin = %q, %v", pinned, err)
	}
	pinned, err = db.PinRCGateImage(gate.ID, gate.CandidateSource, "registry.fly.io/litestream-soak:second")
	if err != nil || pinned != "registry.fly.io/litestream-soak:first" {
		t.Fatalf("second pin = %q, %v; want original image kept", pinned, err)
	}
	pinned, err = db.PinRCGateImage(gate.ID, gate.BaselineSource, "registry.fly.io/litestream-soak:baseline")
	if err != nil || pinned != "registry.fly.io/litestream-soak:baseline" {
		t.Fatalf("baseline pin = %q, %v", pinned, err)
	}
	if _, err := db.PinRCGateImage(gate.ID, "pr-1", "registry.fly.io/litestream-soak:x"); err == nil {
		t.Fatal("pin for unrelated source succeeded")
	}
}
