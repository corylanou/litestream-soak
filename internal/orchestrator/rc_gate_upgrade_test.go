package orchestrator

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

const (
	testUpgradeCandidate = "up-aaaaaaaa"
	testUpgradeBaseline  = "upbase-bbbbbbbb"
)

func registerTestUpgradeGate(t *testing.T, db *model.DB, warmup string, started time.Time) model.RCGate {
	t.Helper()
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	gate, _, err := manager.RegisterRCGate(RCGateRequest{
		Kind:          string(model.RCGateKindUpgrade),
		Repository:    "benbjohnson/litestream",
		CandidateRef:  "pull/1514/head",
		CandidateSHA:  testGateCandidateSHA,
		BaselineSHA:   testGateBaselineSHA,
		Duration:      "24h",
		UpgradeWarmup: warmup,
	}, started)
	if err != nil {
		t.Fatalf("RegisterRCGate() error = %v", err)
	}
	return gate
}

func TestUpgradeGateFleetIsThreeSmallWorkers(t *testing.T) {
	tests := []struct {
		source       string
		wantProfiles []string
	}{
		{source: testUpgradeCandidate, wantProfiles: []string{gateUpgradeLaneProfile, gateUpgradeFollowProfile}},
		{source: testUpgradeBaseline, wantProfiles: []string{gateUpgradeFollowProfile}},
	}
	configs := map[string]string{}
	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			spec := GateFleetForSource(tt.source, testGateSoakSHA, testGateCandidateSHA)
			if len(spec.Workers) != len(tt.wantProfiles) {
				t.Fatalf("workers = %d, want %d", len(spec.Workers), len(tt.wantProfiles))
			}
			for i, worker := range spec.Workers {
				cfg := worker.Workload
				if worker.ProfileName != tt.wantProfiles[i] || worker.Source != tt.source || worker.LitestreamSHA != testGateCandidateSHA || worker.GitSHA != testGateSoakSHA {
					t.Fatalf("worker %d = %+v", i, worker)
				}
				if worker.Name != "worker-"+tt.source+"-"+worker.ProfileName || worker.WorkerID != worker.Name {
					t.Fatalf("name = %q id = %q", worker.Name, worker.WorkerID)
				}
				if worker.Region != "ord" || cfg.CPUs != 1 || cfg.MemoryMB != 1024 || worker.VolumeSizeGB != 10 || cfg.VolumeSizeGB != 10 {
					t.Fatalf("%s resources = region %s cpus %d memory %d volume %d/%d", worker.Name, worker.Region, cfg.CPUs, cfg.MemoryMB, worker.VolumeSizeGB, cfg.VolumeSizeGB)
				}
				if !cfg.FollowCheck || !cfg.CompactionProgressCheck || !cfg.ReplicaLevelReporting || cfg.CompactionL0ObjectBound == 0 || cfg.CompactionL1ObjectBound == 0 {
					t.Fatalf("%s checks = %+v", worker.Name, cfg)
				}
				if cfg.L1CompactionInterval == "" || cfg.L2CompactionInterval == "" || cfg.L3CompactionInterval == "" || cfg.L0Retention == "" {
					t.Fatalf("%s compaction levels = %+v", worker.Name, cfg)
				}
				if previous, ok := configs[worker.ProfileName]; ok && previous != cfg.JSON() {
					t.Fatalf("%s config differs between candidate and baseline:\n%s\n%s", worker.ProfileName, previous, cfg.JSON())
				}
				configs[worker.ProfileName] = cfg.JSON()
			}
		})
	}
	lane := gateWorker(t, GateFleetForSource(testUpgradeCandidate, testGateSoakSHA, testGateCandidateSHA), gateUpgradeLaneProfile).Workload
	if !lane.UpgradeLane || lane.UpgradeWarmup != "2h0m0s" || lane.SnapshotInterval != "24h" || lane.UpgradePostSnapshotInterval != "1h" {
		t.Fatalf("lane workload = %+v", lane)
	}
	follow := gateWorker(t, GateFleetForSource(testUpgradeCandidate, testGateSoakSHA, testGateCandidateSHA), gateUpgradeFollowProfile).Workload
	if follow.UpgradeLane || follow.UpgradeWarmup != "" || follow.SnapshotInterval != "15m" {
		t.Fatalf("follow workload = %+v", follow)
	}
	for _, worker := range GateFleetForSource(testGateCandidate, testGateSoakSHA, testGateCandidateSHA).Workers {
		if worker.Workload.UpgradeLane || worker.Workload.FollowCheck {
			t.Fatalf("rc gate worker gained upgrade settings: %+v", worker)
		}
	}
}

func TestUpgradeGateSourcesAndKinds(t *testing.T) {
	for source, want := range map[string]struct {
		gate bool
		kind model.RCGateKind
	}{
		testUpgradeCandidate: {true, model.RCGateKindUpgrade},
		testUpgradeBaseline:  {true, model.RCGateKindUpgrade},
		testGateCandidate:    {true, model.RCGateKindRC},
		testGateBaseline:     {true, model.RCGateKindRC},
		"up-abc":             {false, model.RCGateKindUpgrade},
		"upbase-AAAAAAAA":    {false, model.RCGateKindUpgrade},
		"upgrade-aaaaaaaa":   {false, model.RCGateKindRC},
		"main":               {false, model.RCGateKindRC},
		"pr-1514":            {false, model.RCGateKindRC},
	} {
		if got := isGateSource(source); got != want.gate {
			t.Fatalf("isGateSource(%q) = %v, want %v", source, got, want.gate)
		}
		if got := gateKindForSource(source); got != want.kind {
			t.Fatalf("gateKindForSource(%q) = %v, want %v", source, got, want.kind)
		}
		if want.gate && supportsDefaultFleetSource(source) {
			t.Fatalf("gate source %s would receive the default fleet", source)
		}
	}
	for _, label := range []string{
		"registry.fly.io/litestream-soak:sha-cccccccccccc-up-aaaaaaaa-ls-aaaaaaaa1111",
		"registry.fly.io/litestream-soak:sha-cccccccccccc-upbase-bbbbbbbb-ls-bbbbbbbb2222",
	} {
		if !versionedWorkerImageRe.MatchString(label) {
			t.Fatalf("image label %s rejected", label)
		}
	}
}

func TestRegisterUpgradeGate(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	base := RCGateRequest{
		Kind:         string(model.RCGateKindUpgrade),
		Repository:   "benbjohnson/litestream",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
	}
	tests := []struct {
		name       string
		mutate     func(*RCGateRequest)
		wantErr    string
		wantWarmup time.Duration
		wantTTL    time.Duration
	}{
		{name: "defaults to 24h with a 2h warm-up", wantWarmup: 2 * time.Hour, wantTTL: 24 * time.Hour},
		{name: "explicit warm-up", mutate: func(r *RCGateRequest) { r.Duration = "12h"; r.UpgradeWarmup = "30m" }, wantWarmup: 30 * time.Minute, wantTTL: 12 * time.Hour},
		{name: "warm-up must end before the TTL", mutate: func(r *RCGateRequest) { r.Duration = "2h"; r.UpgradeWarmup = "2h" }, wantErr: "must be shorter than duration"},
		{name: "tail is not offered", mutate: func(r *RCGateRequest) { r.Tail = true }, wantErr: "do not support tail"},
		{name: "unknown kind", mutate: func(r *RCGateRequest) { r.Kind = "canary" }, wantErr: "kind must be"},
		{name: "warm-up is upgrade only", mutate: func(r *RCGateRequest) { r.Kind = ""; r.Duration = "72h"; r.UpgradeWarmup = "2h" }, wantErr: "only valid for upgrade gates"},
		{name: "bad warm-up", mutate: func(r *RCGateRequest) { r.UpgradeWarmup = "soon" }, wantErr: "upgrade_warmup must be a positive duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
			request := base
			if tt.mutate != nil {
				tt.mutate(&request)
			}
			gate, created, err := manager.RegisterRCGate(request, now)
			if tt.wantErr != "" {
				if !errors.Is(err, errRCGateInvalid) || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || !created {
				t.Fatalf("RegisterRCGate() = %v, %v", created, err)
			}
			if gate.Kind != model.RCGateKindUpgrade || gate.CandidateSource != testUpgradeCandidate || gate.BaselineSource != testUpgradeBaseline {
				t.Fatalf("gate = %+v", gate)
			}
			if gate.UpgradeWarmup != tt.wantWarmup || gate.Duration != tt.wantTTL || !gate.ExpiresAt.Equal(now.Add(tt.wantTTL)) || gate.Tail {
				t.Fatalf("gate timing = warm-up %s duration %s expires %s tail %v", gate.UpgradeWarmup, gate.Duration, gate.ExpiresAt, gate.Tail)
			}
			if len(gate.Resources) != 3 {
				t.Fatalf("resources = %+v, want 3 machines", gate.Resources)
			}
			var volume int
			for _, resource := range gate.Resources {
				volume += resource.VolumeGB
				if resource.Tail || resource.CPUs != 1 || resource.Region != "ord" {
					t.Fatalf("resource = %+v", resource)
				}
			}
			if volume != 30 {
				t.Fatalf("total volume = %d GB, want 30", volume)
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil || stored.Kind != model.RCGateKindUpgrade || stored.UpgradeWarmup != tt.wantWarmup {
				t.Fatalf("stored gate = %+v, %v", stored, err)
			}
			again, created, err := manager.RegisterRCGate(request, now.Add(time.Minute))
			if err != nil || created || again.ID != gate.ID {
				t.Fatalf("repeat registration = %+v created %v err %v", again, created, err)
			}
			changed := request
			changed.UpgradeWarmup = "45m"
			if _, _, err := manager.RegisterRCGate(changed, now.Add(time.Minute)); !errors.Is(err, model.ErrRCGateSourceActive) {
				t.Fatalf("changed warm-up err = %v, want ErrRCGateSourceActive", err)
			}
			entry, err := buildRCGateLedgerEntry(db, *stored, now)
			if err != nil || entry.UpgradeWarmup != tt.wantWarmup.String() || entry.TotalMachines != 3 {
				t.Fatalf("ledger entry = %+v, %v", entry, err)
			}
		})
	}
}

func TestUpgradeGateFleetAppliesRegisteredWarmup(t *testing.T) {
	db := openTestDB(t)
	gate := registerTestUpgradeGate(t, db, "45m", time.Now().UTC())
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	for source, wantProfiles := range map[string]int{testUpgradeCandidate: 2, testUpgradeBaseline: 1} {
		spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, testGateCandidateSHA, gate.StartedAt.Add(time.Minute))
		if err != nil || len(spec.Workers) != wantProfiles {
			t.Fatalf("%s fleet = %+v, %v", source, spec, err)
		}
		for _, worker := range spec.Workers {
			if worker.Workload.UpgradeLane && worker.Workload.UpgradeWarmup != "45m0s" {
				t.Fatalf("lane warm-up = %q, want the registered 45m", worker.Workload.UpgradeWarmup)
			}
		}
	}
	for _, tc := range []struct {
		source, profile string
		want            bool
	}{
		{testUpgradeCandidate, gateUpgradeLaneProfile, true},
		{testUpgradeCandidate, gateUpgradeFollowProfile, true},
		{testUpgradeBaseline, gateUpgradeFollowProfile, true},
		{testUpgradeCandidate, gateStressProfile, false},
	} {
		if got := manager.rcGateProfilePermitted(gate, tc.profile, gate.StartedAt.Add(time.Minute)); got != tc.want {
			t.Fatalf("%s %s permitted = %v, want %v", tc.source, tc.profile, got, tc.want)
		}
	}
}

func recordUpgradeGateVerification(t *testing.T, db *model.DB, source, profile, phase, activeSHA, checkType, mode, status string, at time.Time) {
	t.Helper()
	worker, err := db.GetWorker("worker-" + source + "-" + profile)
	if err != nil {
		t.Fatal(err)
	}
	deployment, err := db.GetDeploymentByVersion(source, worker.GitSHA, worker.LitestreamSHA)
	if err != nil || deployment == nil {
		t.Fatalf("deployment for %s: %v", source, err)
	}
	run := fixtureRun(*worker, *deployment)
	run.UpgradePhase = phase
	run.ActiveLitestreamSHA = activeSHA
	if phase != "" && activeSHA == "" {
		run.ActiveLitestreamSHA = testGateBaselineSHA
		if phase == "post-upgrade" {
			run.ActiveLitestreamSHA = testGateCandidateSHA
		}
	}
	mustRecordVerification(t, db, &model.Verification{
		WorkerID: worker.ID, StartedAt: at, CompletedAt: &at, Status: status, Passed: status == "passed", CheckType: checkType, RestoreMode: mode, Run: run, Attributed: true,
	})
}

func TestUpgradeGateLedgerScoresPhasesAndFollow(t *testing.T) {
	type verification struct {
		source, profile, phase, activeSHA, checkType, mode, status string
	}
	outcome := func(passed bool) string {
		if passed {
			return "passed"
		}
		return "failed"
	}
	laneRestore := func(phase, mode string, passed bool) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, phase, "", "integrity", mode, outcome(passed)}
	}
	laneAborted := func(phase, mode string) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, phase, "", "integrity", mode, "aborted"}
	}
	historical := func(status string) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, "post-upgrade", "", "pre-upgrade-restore", "txid", status}
	}
	mislabelled := func(phase, activeSHA, mode string, passed bool) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, phase, activeSHA, "integrity", mode, outcome(passed)}
	}
	laneRotation := func(phase string) []verification {
		return []verification{laneRestore(phase, "txid", true), laneRestore(phase, "timestamp", true), laneRestore(phase, "latest", true)}
	}
	laneFollow := func(passed bool) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, "post-upgrade", "", "follow", "follow", outcome(passed)}
	}
	follow := func(source string, checkType, mode string, passed bool) verification {
		return verification{source, gateUpgradeFollowProfile, "", "", checkType, mode, outcome(passed)}
	}
	followRotation := func(source string) []verification {
		return []verification{follow(source, "integrity", "txid", true), follow(source, "integrity", "timestamp", true), follow(source, "integrity", "latest", true)}
	}
	laneCoverage := func(phase string, passed bool) verification {
		return verification{testUpgradeCandidate, gateUpgradeLaneProfile, phase, "", "coverage", "coverage", outcome(passed)}
	}
	followCoverage := func(source string, passed bool) verification {
		return follow(source, "coverage", "coverage", passed)
	}
	coverage := []verification{laneCoverage("pre-upgrade", true), laneCoverage("post-upgrade", true), followCoverage(testUpgradeCandidate, true), followCoverage(testUpgradeBaseline, true)}
	concat := func(groups ...[]verification) []verification {
		var all []verification
		for _, group := range groups {
			all = append(all, group...)
		}
		return all
	}
	fullCoverage := []verification{
		laneFollow(true), historical("passed"),
		follow(testUpgradeCandidate, "integrity", "txid", true), follow(testUpgradeCandidate, "integrity", "timestamp", true), follow(testUpgradeCandidate, "integrity", "latest", true), follow(testUpgradeCandidate, "follow", "follow", true),
		follow(testUpgradeBaseline, "integrity", "txid", true), follow(testUpgradeBaseline, "integrity", "timestamp", true), follow(testUpgradeBaseline, "integrity", "latest", true), follow(testUpgradeBaseline, "follow", "follow", true),
	}
	fullCoverage = concat(laneRotation("pre-upgrade"), laneRotation("post-upgrade"), coverage, fullCoverage)
	tests := []struct {
		name          string
		verifications []verification
		wantVerdict   string
		wantScenarios map[string]string
	}{
		{
			name:          "full coverage passes",
			verifications: fullCoverage,
			wantVerdict:   rcGateVerdictPass,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "post-upgrade restore failure is a regression against the pre-upgrade phase",
			verifications: append(append([]verification{}, fullCoverage...), laneRestore("post-upgrade", "latest", false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictRegression, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "lane follow failure compares against the baseline follow worker",
			verifications: append(append([]verification{}, fullCoverage...), laneFollow(false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictRegression, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name: "no post-upgrade evidence is incomplete",
			verifications: concat(coverage, laneRotation("pre-upgrade"), followRotation(testUpgradeCandidate), followRotation(testUpgradeBaseline),
				[]verification{follow(testUpgradeCandidate, "follow", "follow", true), follow(testUpgradeBaseline, "follow", "follow", true)}),
			wantVerdict:   rcGateVerdictIncomplete,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictIncomplete, "upgrade-lane-historical": rcGateVerdictIncomplete, "upgrade-lane-follow": rcGateVerdictIncomplete, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name: "follow workers without follow coverage are incomplete",
			verifications: concat(coverage, laneRotation("pre-upgrade"), laneRotation("post-upgrade"), followRotation(testUpgradeCandidate), followRotation(testUpgradeBaseline),
				[]verification{laneFollow(true), historical("passed"), follow(testUpgradeBaseline, "follow", "follow", true)}),
			wantVerdict:   rcGateVerdictIncomplete,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictIncomplete, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name: "post-upgrade failures reported with the base SHA do not count",
			verifications: append(append([]verification{}, fullCoverage...),
				mislabelled("post-upgrade", testGateBaselineSHA, "txid", false),
				mislabelled("pre-upgrade", testGateCandidateSHA, "txid", false),
				mislabelled("post-upgrade", "unknown", "latest", false)),
			wantVerdict:   rcGateVerdictPass,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name: "post-upgrade passes reported with the base SHA do not give coverage",
			verifications: concat(coverage, laneRotation("pre-upgrade"), followRotation(testUpgradeCandidate), followRotation(testUpgradeBaseline),
				[]verification{laneFollow(true), historical("passed"), follow(testUpgradeCandidate, "follow", "follow", true), follow(testUpgradeBaseline, "follow", "follow", true),
					mislabelled("post-upgrade", testGateBaselineSHA, "txid", true), mislabelled("post-upgrade", testGateBaselineSHA, "timestamp", true), mislabelled("post-upgrade", testGateBaselineSHA, "latest", true)}),
			wantVerdict:   rcGateVerdictIncomplete,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictIncomplete, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name: "historical pass does not stand in for aborted current txid coverage",
			verifications: concat(coverage, laneRotation("pre-upgrade"), followRotation(testUpgradeCandidate), followRotation(testUpgradeBaseline),
				[]verification{
					laneAborted("post-upgrade", "txid"), laneAborted("post-upgrade", "txid"), laneRestore("post-upgrade", "timestamp", true), laneRestore("post-upgrade", "latest", true),
					historical("passed"), historical("passed"), laneFollow(true),
					follow(testUpgradeCandidate, "follow", "follow", true), follow(testUpgradeBaseline, "follow", "follow", true),
				}),
			wantVerdict:   rcGateVerdictIncomplete,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictIncomplete, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "historical restore failure is its own regression",
			verifications: append(append([]verification{}, fullCoverage...), historical("failed")),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictRegression, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "historical target lost is inconclusive, not failing",
			verifications: append(append([]verification{}, fullCoverage...), historical("aborted")),
			wantVerdict:   rcGateVerdictPass,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "post-upgrade ltx coverage hole is a regression against the pre-upgrade phase",
			verifications: append(append([]verification{}, fullCoverage...), laneCoverage("post-upgrade", false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictRegression, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
		{
			name:          "candidate follow worker hole is a regression and does not leak into follow-l9",
			verifications: append(append([]verification{}, fullCoverage...), followCoverage(testUpgradeCandidate, false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictRegression},
		},
		{
			name:          "holes on both follow workers are both_failing",
			verifications: append(append([]verification{}, fullCoverage...), followCoverage(testUpgradeCandidate, false), followCoverage(testUpgradeBaseline, false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictPass, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictBothFailing},
		},
		{
			name:          "both follow lanes failing is not a regression",
			verifications: append(append([]verification{}, fullCoverage...), follow(testUpgradeCandidate, "follow", "follow", false), follow(testUpgradeBaseline, "follow", "follow", false)),
			wantVerdict:   rcGateVerdictFail,
			wantScenarios: map[string]string{"upgrade-lane": rcGateVerdictPass, "upgrade-lane-historical": rcGateVerdictPass, "upgrade-lane-follow": rcGateVerdictPass, "follow-l9": rcGateVerdictBothFailing, "upgrade-lane-ltx-coverage": rcGateVerdictPass, "follow-l9-ltx-coverage": rcGateVerdictPass},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			gate := registerTestUpgradeGate(t, db, "", time.Now().UTC().Add(-3*time.Hour))
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
			if results.Verdict != tt.wantVerdict {
				t.Fatalf("verdict = %s, want %s: %+v", results.Verdict, tt.wantVerdict, results.Scenarios)
			}
			if len(results.Scenarios) != len(tt.wantScenarios) {
				t.Fatalf("scenarios = %+v", results.Scenarios)
			}
			for _, scenario := range results.Scenarios {
				if want := tt.wantScenarios[scenario.Scenario]; scenario.Verdict != want {
					t.Fatalf("%s verdict = %s, want %s: %+v", scenario.Scenario, scenario.Verdict, want, scenario)
				}
				if (scenario.Scenario == "upgrade-lane" || scenario.Scenario == "upgrade-lane-historical") && (scenario.Candidate.Source != testUpgradeCandidate || scenario.Baseline.Source != testUpgradeCandidate) {
					t.Fatalf("upgrade-lane sides = %+v / %+v, want both from the lane worker", scenario.Candidate, scenario.Baseline)
				}
				if scenario.Scenario == "upgrade-lane-follow" && scenario.Baseline.Source != testUpgradeBaseline {
					t.Fatalf("upgrade-lane-follow baseline = %+v, want the baseline follow worker", scenario.Baseline)
				}
			}
		})
	}
}

func TestUpgradeGateWorkerEnvCarriesLaneSettings(t *testing.T) {
	manager := NewManager(nil, openTestDB(t), nil, nil, "litestream-soak", ReplicaConfig{Bucket: "bucket", Endpoint: "https://example.test"}, "", "")
	lane := gateWorker(t, GateFleetForSource(testUpgradeCandidate, testGateSoakSHA, testGateCandidateSHA), gateUpgradeLaneProfile)
	env := manager.workerEnv(model.Worker{ID: lane.WorkerID, Name: lane.Name, Source: testUpgradeCandidate, ProfileName: lane.ProfileName, LitestreamSHA: testGateCandidateSHA}, normalizeWorkloadConfig(lane.Workload))
	for key, want := range map[string]string{
		"UPGRADE_LANE":                   "true",
		"UPGRADE_WARMUP":                 "2h0m0s",
		"UPGRADE_POST_SNAPSHOT_INTERVAL": "1h",
		"FOLLOW_CHECK":                   "true",
		"COMPACTION_PROGRESS_CHECK":      "true",
		"COMPACTION_L0_OBJECT_BOUND":     "2000",
		"COMPACTION_L1_OBJECT_BOUND":     "6000",
		"SNAPSHOT_INTERVAL":              "24h",
		"LITESTREAM_SHA":                 testGateCandidateSHA,
	} {
		if env[key] != want {
			t.Fatalf("env[%s] = %q, want %q", key, env[key], want)
		}
	}
	follow := gateWorker(t, GateFleetForSource(testUpgradeBaseline, testGateSoakSHA, testGateBaselineSHA), gateUpgradeFollowProfile)
	env = manager.workerEnv(model.Worker{ID: follow.WorkerID, Name: follow.Name, Source: testUpgradeBaseline, ProfileName: follow.ProfileName, LitestreamSHA: testGateBaselineSHA}, normalizeWorkloadConfig(follow.Workload))
	for _, key := range []string{"UPGRADE_LANE", "UPGRADE_WARMUP", "UPGRADE_POST_SNAPSHOT_INTERVAL"} {
		if _, ok := env[key]; ok {
			t.Fatalf("follow worker env has %s", key)
		}
	}
	if env["FOLLOW_CHECK"] != "true" || env["SNAPSHOT_INTERVAL"] != "15m" {
		t.Fatalf("follow worker env = %+v", env)
	}
}
