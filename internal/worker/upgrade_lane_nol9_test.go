package worker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

// orderedReplicaStore records every lane event in order so the tests can
// prove the L9 purge runs strictly between the base run returning and the
// candidate run starting.
type orderedReplicaStore struct {
	fakeReplicaStore
	events *[]string
}

func (s *orderedReplicaStore) purger() replicaL9Purger {
	inner := s.fakeReplicaStore.purger()
	return replicaL9Purger{
		list: func(ctx context.Context, cfg Config) ([]string, error) {
			*s.events = append(*s.events, "list")
			return inner.list(ctx, cfg)
		},
		remove: func(ctx context.Context, cfg Config, key string) error {
			*s.events = append(*s.events, "delete "+key)
			return inner.remove(ctx, cfg, key)
		},
	}
}

func TestUpgradeLaneNoL9PurgeOrdering(t *testing.T) {
	own := "soak/up-nol9-a25ac011/worker-up-nol9-a25ac011-gate-upgrade-lane-nol9/vol-1/"
	l9 := own + "0009/0000000000000001-0000000000000001.ltx"
	l1 := own + "0001/0000000000000001-0000000000000040.ltx"
	start := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		deleteL9    bool
		state       *upgradeLaneState
		keys        []string
		listErr     error
		wantEvents  []string
		wantErr     string
		wantPurged  bool
		wantPostRun bool
	}{
		{
			name:        "purge runs after base stops and before the candidate starts",
			deleteL9:    true,
			keys:        []string{l1, l9},
			wantEvents:  []string{"pre start", "pre stopped", "list", "delete " + l9, "list", "post start"},
			wantPurged:  true,
			wantPostRun: true,
		},
		{
			name:        "standard lane never deletes",
			keys:        []string{l1, l9},
			wantEvents:  []string{"pre start", "pre stopped", "post start"},
			wantPostRun: true,
		},
		{
			name:        "restart after the switch without a recorded purge purges before the candidate",
			deleteL9:    true,
			state:       &upgradeLaneState{Phase: UpgradePhasePost, StartedAt: start.Add(-3 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA},
			keys:        []string{l1, l9},
			wantEvents:  []string{"list", "delete " + l9, "list", "post start"},
			wantPurged:  true,
			wantPostRun: true,
		},
		{
			name:     "restart after a recorded purge does not purge again",
			deleteL9: true,
			state: &upgradeLaneState{Phase: UpgradePhasePost, StartedAt: start.Add(-3 * time.Hour), BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA,
				L9Purge: &replicaL9Purge{StartedAt: start, CompletedAt: start, Prefix: strings.TrimSuffix(own, "/"), Deleted: []string{l9}}},
			keys:        []string{l1, own + "0009/0000000000000001-0000000000000090.ltx"},
			wantEvents:  []string{"post start"},
			wantPurged:  true,
			wantPostRun: true,
		},
		{
			name:       "purge failure keeps the candidate from starting",
			deleteL9:   true,
			listErr:    errors.New("s3cmd: access denied"),
			wantEvents: []string{"pre start", "pre stopped", "list"},
			wantErr:    "upgrade lane L9 purge",
			wantPurged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := upgradeLaneConfig(t, dir)
			cfg.UpgradeWarmup = 50 * time.Millisecond
			cfg.UpgradeDeleteL9AtSwitch = tt.deleteL9
			cfg.ReplicaType = "s3"
			cfg.S3Bucket = "bucket"
			cfg.S3Path = strings.TrimSuffix(own, "/")
			if tt.state != nil {
				if err := writeUpgradeLaneState(dir, *tt.state); err != nil {
					t.Fatal(err)
				}
			}
			var events []string
			store := &orderedReplicaStore{fakeReplicaStore: fakeReplicaStore{keys: slices.Clone(tt.keys), listErr: tt.listErr}, events: &events}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var postCfg *Config
			run := func(runCtx context.Context, phase Config) error {
				switch phase.upgradePhase {
				case UpgradePhasePre:
					events = append(events, "pre start")
					<-runCtx.Done()
					events = append(events, "pre stopped")
					if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
						return cause
					}
					return nil
				default:
					events = append(events, "post start")
					copied := phase
					postCfg = &copied
					cancel()
					return nil
				}
			}
			err := runUpgradeLane(ctx, cfg, func() time.Time { return start }, store.purger(), run)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("runUpgradeLane() error = %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("runUpgradeLane() error = %v, want %q", err, tt.wantErr)
			}
			if !slices.Equal(events, tt.wantEvents) {
				t.Fatalf("events = %v, want %v", events, tt.wantEvents)
			}
			if (postCfg != nil) != tt.wantPostRun {
				t.Fatalf("post run = %v, want %v", postCfg != nil, tt.wantPostRun)
			}
			state, err := readUpgradeLaneState(dir)
			if err != nil {
				t.Fatal(err)
			}
			if (state.L9Purge != nil) != tt.wantPurged {
				t.Fatalf("state purge = %+v, want recorded %v", state.L9Purge, tt.wantPurged)
			}
			if tt.wantPostRun && tt.deleteL9 {
				remaining, err := store.purger().list(context.Background(), *postCfg)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range remaining {
					if level, ok := replicaLevelOfKey(key); ok && level == replicaL9Level && state.L9Purge.Error == "" && tt.state == nil {
						t.Fatalf("candidate started with an L9 object on the replica: %s", key)
					}
				}
				if tt.state == nil && (len(state.L9Purge.Deleted) != 1 || state.L9Purge.Deleted[0] != l9 || state.L9Purge.RemainingL9 != 0) {
					t.Fatalf("purge record = %+v", state.L9Purge)
				}
				if postCfg.litestreamCommand() != "litestream" || postCfg.activeLitestreamSHA != testUpgradeCandidateSHA {
					t.Fatalf("post config = binary %s sha %s", postCfg.litestreamCommand(), postCfg.activeLitestreamSHA)
				}
			}
			if tt.wantErr != "" && state.L9Purge != nil && state.L9Purge.Error == "" {
				t.Fatalf("failed purge recorded without its error: %+v", state.L9Purge)
			}
		})
	}
}

func TestRecordFirstCandidateL9(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)
	empty := reporting.ReplicaLevelSummary{Level: 9}
	written := reporting.ReplicaLevelSummary{Level: 9, ObjectCount: 1, MaxTXIDDecimal: 0x900}
	if err := writeUpgradeLaneState(dir, upgradeLaneState{Phase: UpgradePhasePost, StartedAt: now, BaseSHA: testUpgradeBaseSHA, CandidateSHA: testUpgradeCandidateSHA}); err != nil {
		t.Fatal(err)
	}
	if first, err := recordFirstCandidateL9(dir, written, now); err != nil || first != nil {
		t.Fatalf("without a purge record = %+v, %v; want nothing recorded", first, err)
	}
	state, _ := readUpgradeLaneState(dir)
	state.L9Purge = &replicaL9Purge{StartedAt: now, CompletedAt: now, Prefix: "soak/lane", Deleted: []string{}}
	if err := writeUpgradeLaneState(dir, state); err != nil {
		t.Fatal(err)
	}
	if first, err := recordFirstCandidateL9(dir, empty, now); err != nil || first != nil {
		t.Fatalf("empty L9 = %+v, %v; want nothing recorded", first, err)
	}
	first, err := recordFirstCandidateL9(dir, written, now.Add(time.Hour))
	if err != nil || first == nil || first.MaxTXID != "0000000000000900" || !first.ObservedAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("first L9 = %+v, %v", first, err)
	}
	later := reporting.ReplicaLevelSummary{Level: 9, ObjectCount: 2, MaxTXIDDecimal: 0xa00}
	again, err := recordFirstCandidateL9(dir, later, now.Add(2*time.Hour))
	if err != nil || again.MaxTXID != "0000000000000900" {
		t.Fatalf("first L9 moved: %+v, %v", again, err)
	}
	cfg := upgradeLaneConfig(t, dir)
	cfg.UpgradeDeleteL9AtSwitch = true
	cfg.upgradePhase = UpgradePhasePost
	r := &Runner{cfg: cfg}
	if note := r.candidateL9Note(later, now); !strings.Contains(note, "observed "+now.Add(time.Hour).Format(time.RFC3339)) {
		t.Fatalf("note = %q", note)
	}
}
