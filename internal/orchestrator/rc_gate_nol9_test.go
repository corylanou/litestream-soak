package orchestrator

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

const (
	testNoL9Candidate = "up-nol9-aaaaaaaa"
	testNoL9Baseline  = "upbase-nol9-bbbbbbbb"
)

func registerTestNoL9Gate(t *testing.T, db *model.DB, started time.Time) model.RCGate {
	t.Helper()
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Kind:         string(model.RCGateKindUpgrade),
		LaneVariant:  upgradeLaneVariantNoL9,
		Repository:   "benbjohnson/litestream",
		CandidateRef: "pull/1514/head",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
	}, started)
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	return gate
}

func TestNoL9GateFleetAndSources(t *testing.T) {
	candidate := GateFleetForSource(testNoL9Candidate, testGateSoakSHA, testGateCandidateSHA)
	if len(candidate.Workers) != 1 {
		t.Fatalf("candidate workers = %+v, want the no-L9 lane only", candidate.Workers)
	}
	lane := candidate.Workers[0]
	if lane.ProfileName != gateUpgradeLaneNoL9Profile || lane.Name != "worker-"+testNoL9Candidate+"-"+gateUpgradeLaneNoL9Profile || lane.WorkerID != lane.Name {
		t.Fatalf("lane = %+v", lane)
	}
	if !lane.Workload.UpgradeLane || !lane.Workload.UpgradeDeleteL9AtSwitch || !lane.Workload.FollowCheck || !lane.Workload.CompactionProgressCheck || lane.Workload.SnapshotInterval != "24h" {
		t.Fatalf("lane workload = %+v", lane.Workload)
	}
	standard := gateWorker(t, GateFleetForSource(testUpgradeCandidate, testGateSoakSHA, testGateCandidateSHA), gateUpgradeLaneProfile).Workload
	if standard.UpgradeDeleteL9AtSwitch {
		t.Fatal("standard lane deletes L9")
	}
	standard.UpgradeDeleteL9AtSwitch = true
	if standard.JSON() != lane.Workload.JSON() {
		t.Fatalf("no-L9 lane differs from the standard lane beyond the purge flag:\n%s\n%s", standard.JSON(), lane.Workload.JSON())
	}
	baseline := GateFleetForSource(testNoL9Baseline, testGateSoakSHA, testGateBaselineSHA)
	if len(baseline.Workers) != 1 || baseline.Workers[0].ProfileName != gateUpgradeFollowProfile || baseline.Workers[0].Workload.UpgradeLane {
		t.Fatalf("baseline workers = %+v, want the base follow worker only", baseline.Workers)
	}

	for source, want := range map[string]struct {
		gate    bool
		kind    model.RCGateKind
		variant string
	}{
		testNoL9Candidate:    {true, model.RCGateKindUpgrade, upgradeLaneVariantNoL9},
		testNoL9Baseline:     {true, model.RCGateKindUpgrade, upgradeLaneVariantNoL9},
		testUpgradeCandidate: {true, model.RCGateKindUpgrade, ""},
		testGateCandidate:    {true, model.RCGateKindRC, ""},
		"up-nol9":            {false, model.RCGateKindUpgrade, ""},
		"up-fast-aaaaaaaa":   {false, model.RCGateKindUpgrade, ""},
		"rc-nol9-aaaaaaaa":   {true, model.RCGateKindRC, upgradeLaneVariantNoL9},
		"upnol9-aaaaaaaa":    {false, model.RCGateKindRC, ""},
		"up-nol9-AAAAAAAA":   {false, model.RCGateKindUpgrade, upgradeLaneVariantNoL9},
	} {
		if got := isGateSource(source); got != want.gate {
			t.Fatalf("isGateSource(%q) = %v, want %v", source, got, want.gate)
		}
		if got := gateKindForSource(source); got != want.kind {
			t.Fatalf("gateKindForSource(%q) = %v, want %v", source, got, want.kind)
		}
		if got := gateLaneVariantForSource(source); got != want.variant {
			t.Fatalf("gateLaneVariantForSource(%q) = %q, want %q", source, got, want.variant)
		}
		if want.gate && supportsDefaultFleetSource(source) {
			t.Fatalf("gate source %s would receive the default fleet", source)
		}
	}
	for _, label := range []string{
		"registry.fly.io/litestream-soak:sha-cccccccccccc-up-nol9-aaaaaaaa-ls-aaaaaaaa1111",
		"registry.fly.io/litestream-soak:sha-cccccccccccc-upbase-nol9-bbbbbbbb-ls-bbbbbbbb2222",
	} {
		if !versionedWorkerImageRe.MatchString(label) {
			t.Fatalf("image label %s rejected", label)
		}
	}
}

func TestRegisterNoL9Gate(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate := registerTestNoL9Gate(t, db, now)
	if gate.LaneVariant != upgradeLaneVariantNoL9 || gate.CandidateSource != testNoL9Candidate || gate.BaselineSource != testNoL9Baseline {
		t.Fatalf("gate = %+v", gate)
	}
	if len(gate.Resources) != 2 {
		t.Fatalf("resources = %+v, want the lane and the base follow worker", gate.Resources)
	}
	stored, err := db.GetRCGate(gate.ID)
	if err != nil || stored.LaneVariant != upgradeLaneVariantNoL9 {
		t.Fatalf("stored gate = %+v, %v", stored, err)
	}
	if scenarios := gateScenariosFor(*stored); len(scenarios) != 4 || scenarios[0].name != "upgrade-lane-nol9" {
		t.Fatalf("scenarios = %+v", scenarios)
	}

	// The standard upgrade gate for the same commits does not collide with
	// the no-L9 gate: different sources, both open at once.
	standard := registerTestUpgradeGate(t, db, "", now)
	if standard.CandidateSource != testUpgradeCandidate || standard.ID == gate.ID {
		t.Fatalf("standard gate = %+v", standard)
	}
	if again, created, err := manager.RegisterRCGate(RCGateRequest{
		Kind: string(model.RCGateKindUpgrade), LaneVariant: upgradeLaneVariantNoL9, Repository: "benbjohnson/litestream", CandidateRef: "pull/1514/head",
		CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "24h",
	}, now.Add(time.Minute)); err != nil || created || again.ID != gate.ID {
		t.Fatalf("repeat registration = %+v created %v err %v", again, created, err)
	}

	for _, tc := range []struct {
		name    string
		request RCGateRequest
		wantErr string
	}{
		{name: "unknown variant", request: RCGateRequest{Kind: "upgrade", LaneVariant: "fast", Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA}, wantErr: "lane_variant must be empty or nol9"},
		{name: "variant on an rc gate", request: RCGateRequest{LaneVariant: "nol9", Repository: "benbjohnson/litestream", CandidateSHA: testGateCandidateSHA, BaselineSHA: testGateBaselineSHA, Duration: "72h"}, wantErr: "only valid for upgrade gates"},
	} {
		if _, _, err := manager.RegisterRCGate(tc.request, now); !errors.Is(err, errRCGateInvalid) || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("%s: error = %v, want %q", tc.name, err, tc.wantErr)
		}
	}
}

func TestNoL9GateLedgerScoresLaneTrio(t *testing.T) {
	type verification struct {
		source, profile, phase, activeSHA, checkType, mode, status string
	}
	lane := func(phase, checkType, mode, status string) verification {
		return verification{testNoL9Candidate, gateUpgradeLaneNoL9Profile, phase, "", checkType, mode, status}
	}
	rotation := func(phase string) []verification {
		return []verification{lane(phase, "integrity", "txid", "passed"), lane(phase, "integrity", "timestamp", "passed"), lane(phase, "integrity", "latest", "passed")}
	}
	baseFollow := func(status string) verification {
		return verification{testNoL9Baseline, gateUpgradeFollowProfile, "", "", "follow", "follow", status}
	}
	coverage := []verification{lane("pre-upgrade", "coverage", "coverage", "passed"), lane("post-upgrade", "coverage", "coverage", "passed")}
	full := append(append(append(rotation("pre-upgrade"), rotation("post-upgrade")...), coverage...),
		lane("post-upgrade", "pre-upgrade-restore", "txid", "passed"), lane("post-upgrade", "follow", "follow", "passed"), baseFollow("passed"))
	tests := []struct {
		name          string
		verifications []verification
		wantVerdict   string
		wantScenarios map[string]string
	}{
		{
			name:          "full coverage passes",
			verifications: full,
			wantVerdict:   rcGateVerdictPass,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictPass, "upgrade-lane-nol9-historical": rcGateVerdictPass, "upgrade-lane-nol9-follow": rcGateVerdictPass, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "compaction stall after the purge is a regression",
			verifications: append(append([]verification{}, full...), lane("post-upgrade", "integrity", "txid", "failed")),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictRegression, "upgrade-lane-nol9-historical": rcGateVerdictPass, "upgrade-lane-nol9-follow": rcGateVerdictPass, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "historical restore failure is its own regression",
			verifications: append(append([]verification{}, full...), lane("post-upgrade", "pre-upgrade-restore", "txid", "failed")),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictPass, "upgrade-lane-nol9-historical": rcGateVerdictRegression, "upgrade-lane-nol9-follow": rcGateVerdictPass, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "follow never crossing an L9 is incomplete",
			verifications: append(append(append(rotation("pre-upgrade"), rotation("post-upgrade")...), coverage...), lane("post-upgrade", "pre-upgrade-restore", "txid", "passed"), lane("post-upgrade", "follow", "follow", "pending"), baseFollow("passed")),
			wantVerdict:   rcGateVerdictIncomplete,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictPass, "upgrade-lane-nol9-historical": rcGateVerdictPass, "upgrade-lane-nol9-follow": rcGateVerdictIncomplete, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "post-upgrade ltx coverage hole is a regression against the pre-upgrade phase",
			verifications: append(append([]verification{}, full...), lane("post-upgrade", "coverage", "coverage", "failed")),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictPass, "upgrade-lane-nol9-historical": rcGateVerdictPass, "upgrade-lane-nol9-follow": rcGateVerdictPass, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictRegression},
		},
		{
			name: "post-upgrade reports with the base SHA do not count",
			verifications: append(append([]verification{}, full...),
				verification{testNoL9Candidate, gateUpgradeLaneNoL9Profile, "post-upgrade", testGateBaselineSHA, "integrity", "txid", "failed"}),
			wantVerdict:   rcGateVerdictPass,
			wantScenarios: map[string]string{"upgrade-lane-nol9": rcGateVerdictPass, "upgrade-lane-nol9-historical": rcGateVerdictPass, "upgrade-lane-nol9-follow": rcGateVerdictPass, "upgrade-lane-nol9-ltx-coverage": rcGateVerdictPass},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestNoL9Gate(t, db, time.Now().UTC().Add(-3*time.Hour))
			createTestGateFleet(t, db, gate)
			inside := gate.StartedAt.Add(time.Hour)
			for i, v := range tt.verifications {
				recordUpgradeGateVerification(t, db, v.source, v.profile, v.phase, v.activeSHA, v.checkType, v.mode, v.status, inside.Add(time.Duration(i)*time.Minute))
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatal(err)
			}
			results, err := buildRCGateResults(db, *stored, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if results.Verdict != tt.wantVerdict || len(results.Scenarios) != len(tt.wantScenarios) {
				t.Fatalf("verdict = %s, want %s: %+v", results.Verdict, tt.wantVerdict, results.Scenarios)
			}
			for _, scenario := range results.Scenarios {
				if want := tt.wantScenarios[scenario.Scenario]; scenario.Verdict != want {
					t.Fatalf("%s verdict = %s, want %s: %+v", scenario.Scenario, scenario.Verdict, want, scenario)
				}
				if scenario.Scenario == "upgrade-lane-nol9-follow" && scenario.Baseline.Source != testNoL9Baseline {
					t.Fatalf("follow baseline = %+v, want the base follow worker", scenario.Baseline)
				}
			}
		})
	}
}
