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
		Repository:        "benbjohnson/litestream",
		CandidateRef:      "v0.5.0-rc1",
		CandidateSource:   "rc-aaaaaaaa",
		CandidateSHA:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaselineSource:    "rcbase-bbbbbbbb",
		BaselineSHA:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Duration:          72 * time.Hour,
		Tail:              true,
		Status:            RCGateRunning,
		StartedAt:         started,
		ExpiresAt:         started.Add(72 * time.Hour),
		ProvisionDeadline: started.Add(2 * time.Hour),
		TailExpiresAt:     new(started.Add(7 * 24 * time.Hour)),
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

	if err := db.UpdateRCGateStatus(first.ID, RCGateCompleted, new(started), `{"verdict":"pass"}`); err != nil {
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

func TestRCGateProfilesRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		profiles []string
		selects  map[string]bool
	}{
		{name: "empty selects every profile", selects: map[string]bool{"gate-stress": true, "gate-large-db": true}},
		{name: "subset is stored and selects only itself", profiles: []string{"gate-large-db"}, selects: map[string]bool{"gate-stress": false, "gate-large-db": true}},
		{name: "two profiles keep their order", profiles: []string{"gate-remote", "gate-large-db"}, selects: map[string]bool{"gate-remote": true, "gate-large-db": true, "gate-stress": false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openRCGateTestDB(t)
			gate := testRCGate(time.Now().UTC())
			gate.Profiles = tt.profiles
			if err := db.CreateRCGate(&gate); err != nil {
				t.Fatalf("CreateRCGate() error = %v", err)
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatalf("GetRCGate() error = %v", err)
			}
			if len(stored.Profiles) != len(tt.profiles) {
				t.Fatalf("Profiles = %v, want %v", stored.Profiles, tt.profiles)
			}
			for i := range tt.profiles {
				if stored.Profiles[i] != tt.profiles[i] {
					t.Fatalf("Profiles = %v, want %v", stored.Profiles, tt.profiles)
				}
			}
			for profile, want := range tt.selects {
				if got := stored.SelectsProfile(profile); got != want {
					t.Fatalf("SelectsProfile(%s) = %v, want %v", profile, got, want)
				}
			}
		})
	}
}

func TestRCGateSuffixedSourcesRegisterWhileLegacyGateIsOpen(t *testing.T) {
	db := openRCGateTestDB(t)
	started := time.Now().UTC()
	first := testRCGate(started)
	if err := db.CreateRCGate(&first); err != nil {
		t.Fatalf("CreateRCGate() error = %v", err)
	}
	second := testRCGate(started)
	second.CandidateSource = "rc-aaaaaaaa-2dfdc1c3"
	second.BaselineSource = "rcbase-bbbbbbbb-2dfdc1c3"
	second.Profiles = []string{"gate-large-db"}
	if err := db.CreateRCGate(&second); err != nil {
		t.Fatalf("CreateRCGate() with suffixed sources error = %v", err)
	}
	for source, wantID := range map[string]int64{"rc-aaaaaaaa": first.ID, "rc-aaaaaaaa-2dfdc1c3": second.ID, "rcbase-bbbbbbbb-2dfdc1c3": second.ID} {
		active, err := db.ActiveRCGateForSource(source)
		if err != nil || active == nil || active.ID != wantID {
			t.Fatalf("ActiveRCGateForSource(%s) = %+v, %v; want gate %d", source, active, err, wantID)
		}
	}
}

func TestRCGatePinBuildIsWriteOnceWhileOpen(t *testing.T) {
	db := openRCGateTestDB(t)
	gate := testRCGate(time.Now().UTC())
	if err := db.CreateRCGate(&gate); err != nil {
		t.Fatalf("CreateRCGate() error = %v", err)
	}
	image, soak, err := db.PinRCGateBuild(gate.ID, gate.CandidateSource, "registry.fly.io/litestream-soak:first", "soak-1")
	if err != nil || image != "registry.fly.io/litestream-soak:first" || soak != "soak-1" {
		t.Fatalf("first pin = %q %q, %v", image, soak, err)
	}
	image, soak, err = db.PinRCGateBuild(gate.ID, gate.CandidateSource, "registry.fly.io/litestream-soak:second", "soak-2")
	if err != nil || image != "registry.fly.io/litestream-soak:first" || soak != "soak-1" {
		t.Fatalf("second pin = %q %q, %v; want original build kept", image, soak, err)
	}
	if _, _, err := db.PinRCGateBuild(gate.ID, "pr-1", "registry.fly.io/litestream-soak:x", "soak"); err == nil {
		t.Fatal("pin for unrelated source succeeded")
	}
	if err := db.UpdateRCGateStatus(gate.ID, RCGateAborted, new(time.Now().UTC()), `{"verdict":"incomplete"}`); err != nil {
		t.Fatal(err)
	}
	image, _, err = db.PinRCGateBuild(gate.ID, gate.BaselineSource, "registry.fly.io/litestream-soak:late", "soak-3")
	if err != nil || image != "" {
		t.Fatalf("closed gate pin = %q, %v; want no pin", image, err)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil || !stored.Closed() || stored.Results != `{"verdict":"incomplete"}` || stored.CompletedAt == nil {
		t.Fatalf("closed gate = %+v, %v", stored, err)
	}
	if active, err := db.ActiveRCGateForSource(gate.CandidateSource); err != nil || active != nil {
		t.Fatalf("aborted gate still active: %+v, %v", active, err)
	}
}
