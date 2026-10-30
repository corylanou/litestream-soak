package orchestrator

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
)

func TestRegisterRCGateProfilesAndSourceSuffix(t *testing.T) {
	now := time.Date(2026, 10, 29, 17, 13, 0, 0, time.UTC)
	base := RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "pull/1514/head",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
	}
	tests := []struct {
		name            string
		mutate          func(*RCGateRequest)
		wantErr         string
		wantCandidate   string
		wantBaseline    string
		wantProfiles    []string
		wantMachines    int
		wantVolumeGB    int
		wantScenarios   []string
		wantNotProfiles []string
	}{
		{
			name:          "default runs all four profiles with legacy sources",
			wantCandidate: testGateCandidate,
			wantBaseline:  testGateBaseline,
			wantMachines:  8,
			wantVolumeGB:  150,
			wantScenarios: []string{"overload-truncate0", "pinned-reader", "manydb-500", "large-db", "remote"},
		},
		{
			name:            "large-db only with a per-gate suffix",
			mutate:          func(r *RCGateRequest) { r.Profiles = []string{gateLargeDBProfile}; r.SourceSuffix = "2dfdc1c3" },
			wantCandidate:   testGateCandidate + "-2dfdc1c3",
			wantBaseline:    testGateBaseline + "-2dfdc1c3",
			wantProfiles:    []string{gateLargeDBProfile},
			wantMachines:    2,
			wantVolumeGB:    80,
			wantScenarios:   []string{"large-db"},
			wantNotProfiles: []string{gateStressProfile, gateManyDB500Profile, gateRemoteProfile},
		},
		{
			name:            "profiles are stored in fleet order without duplicates",
			mutate:          func(r *RCGateRequest) { r.Profiles = []string{gateRemoteProfile, gateStressProfile, " gate-remote "} },
			wantCandidate:   testGateCandidate,
			wantBaseline:    testGateBaseline,
			wantProfiles:    []string{gateStressProfile, gateRemoteProfile},
			wantMachines:    4,
			wantVolumeGB:    40,
			wantScenarios:   []string{"overload-truncate0", "pinned-reader", "remote"},
			wantNotProfiles: []string{gateManyDB500Profile, gateLargeDBProfile},
		},
		{
			name:    "unknown profile is rejected",
			mutate:  func(r *RCGateRequest) { r.Profiles = []string{"gate-huge"} },
			wantErr: `profile "gate-huge" is not part of the rc gate fleet`,
		},
		{
			name:    "tail needs gate-stress in the subset",
			mutate:  func(r *RCGateRequest) { r.Profiles = []string{gateLargeDBProfile}; r.Tail = true },
			wantErr: "tail requires the gate-stress profile",
		},
		{
			name:    "suffix must be short lowercase alphanumerics",
			mutate:  func(r *RCGateRequest) { r.SourceSuffix = "Run_1" },
			wantErr: "source_suffix must be 1 to 8 lowercase letters or digits",
		},
		{
			name:    "suffix longer than eight characters is rejected",
			mutate:  func(r *RCGateRequest) { r.SourceSuffix = "123456789" },
			wantErr: "source_suffix must be 1 to 8 lowercase letters or digits",
		},
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
			if gate.CandidateSource != tt.wantCandidate || gate.BaselineSource != tt.wantBaseline {
				t.Fatalf("sources = %s vs %s, want %s vs %s", gate.CandidateSource, gate.BaselineSource, tt.wantCandidate, tt.wantBaseline)
			}
			if !slices.Equal(gate.Profiles, tt.wantProfiles) {
				t.Fatalf("Profiles = %v, want %v", gate.Profiles, tt.wantProfiles)
			}
			for _, source := range gate.Sources() {
				if !isGateSource(source) {
					t.Fatalf("isGateSource(%s) = false", source)
				}
				if sha, ok := gate.LitestreamSHAFor(source); !ok || sha == "" {
					t.Fatalf("LitestreamSHAFor(%s) = %q, %v", source, sha, ok)
				}
			}
			stored, err := db.GetRCGate(gate.ID)
			if err != nil {
				t.Fatalf("GetRCGate() error = %v", err)
			}
			if !slices.Equal(stored.Profiles, tt.wantProfiles) || len(stored.Resources) != tt.wantMachines {
				t.Fatalf("stored gate = profiles %v resources %d, want %v and %d", stored.Profiles, len(stored.Resources), tt.wantProfiles, tt.wantMachines)
			}
			for _, resource := range stored.Resources {
				if !stored.SelectsProfile(resource.Profile) || !slices.Contains(stored.Sources(), resource.Source) {
					t.Fatalf("resource %+v is outside the selected profiles or sources", resource)
				}
			}
			for _, profile := range tt.wantNotProfiles {
				if gateHasProfile(*stored, profile) {
					t.Fatalf("gateHasProfile(%s) = true for an unselected profile", profile)
				}
				if allowed, _ := rcGateProfileWindow(*stored, profile); allowed {
					t.Fatalf("rcGateProfileWindow(%s) allows provisioning an unselected profile", profile)
				}
			}
			for _, source := range stored.Sources() {
				spec, err := manager.rcGateFleetForSource(source, testGateSoakSHA, "", now.Add(time.Minute))
				if err != nil {
					t.Fatalf("rcGateFleetForSource(%s) error = %v", source, err)
				}
				for _, desired := range spec.Workers {
					if !stored.SelectsProfile(desired.ProfileName) {
						t.Fatalf("%s fleet provisions unselected %s", source, desired.ProfileName)
					}
					if desired.Source != source || !strings.HasPrefix(desired.Name, "worker-"+source+"-") {
						t.Fatalf("%s fleet worker = %+v", source, desired)
					}
				}
				want := tt.wantMachines / 2
				if len(spec.Workers) != want {
					t.Fatalf("%s fleet = %d workers, want %d", source, len(spec.Workers), want)
				}
			}
			entry, err := buildRCGateLedgerEntry(db, *stored, now.Add(time.Hour))
			if err != nil {
				t.Fatalf("buildRCGateLedgerEntry() error = %v", err)
			}
			if entry.TotalMachines != tt.wantMachines || entry.TotalVolumeGB != tt.wantVolumeGB {
				t.Fatalf("ledger machines %d volume %d, want %d and %d", entry.TotalMachines, entry.TotalVolumeGB, tt.wantMachines, tt.wantVolumeGB)
			}
			var scenarios []string
			for _, scenario := range entry.Scenarios {
				scenarios = append(scenarios, scenario.Scenario)
			}
			if !slices.Equal(scenarios, tt.wantScenarios) {
				t.Fatalf("ledger scenarios = %v, want %v", scenarios, tt.wantScenarios)
			}
			again, created, err := manager.RegisterRCGate(request, now.Add(time.Minute))
			if err != nil || created || again.ID != gate.ID {
				t.Fatalf("repeat registration = %+v created %v err %v", again, created, err)
			}
		})
	}
}

func TestSuffixedGateRegistersWhileLegacyGateForSameCommitsIsOpen(t *testing.T) {
	now := time.Date(2026, 10, 29, 17, 13, 0, 0, time.UTC)
	db := openTestDB(t)
	manager := NewManager(nil, db, nil, nil, "litestream-soak", ReplicaConfig{}, "", "")
	first := registerTestGate(t, db, false, now)

	rerun := RCGateRequest{
		Repository:   "benbjohnson/litestream",
		CandidateRef: "pull/1514/head",
		CandidateSHA: testGateCandidateSHA,
		BaselineSHA:  testGateBaselineSHA,
		Duration:     "24h",
		Profiles:     []string{gateLargeDBProfile},
	}
	if _, _, err := manager.RegisterRCGate(rerun, now.Add(time.Hour)); !errors.Is(err, model.ErrRCGateSourceActive) {
		t.Fatalf("rerun without a suffix err = %v, want ErrRCGateSourceActive while the legacy gate is open", err)
	}

	rerun.SourceSuffix = "2dfdc1c3"
	second, created, err := manager.RegisterRCGate(rerun, now.Add(time.Hour))
	if err != nil || !created {
		t.Fatalf("suffixed rerun = created %v, err %v", created, err)
	}
	if second.ID == first.ID || second.CandidateSource != testGateCandidate+"-2dfdc1c3" || second.BaselineSource != testGateBaseline+"-2dfdc1c3" {
		t.Fatalf("second gate = %+v", second)
	}
	for source, wantID := range map[string]int64{
		first.CandidateSource:  first.ID,
		first.BaselineSource:   first.ID,
		second.CandidateSource: second.ID,
		second.BaselineSource:  second.ID,
	} {
		active, err := db.ActiveRCGateForSource(source)
		if err != nil || active == nil || active.ID != wantID {
			t.Fatalf("ActiveRCGateForSource(%s) = %+v, %v; want gate %d", source, active, err, wantID)
		}
		if gateKindForSource(source) != model.RCGateKindRC {
			t.Fatalf("gateKindForSource(%s) = %s", source, gateKindForSource(source))
		}
	}
	active, err := db.ActiveRCGates()
	if err != nil || len(active) != 2 {
		t.Fatalf("ActiveRCGates() = %d, %v; want both gates open", len(active), err)
	}

	createTestGateFleet(t, db, second)
	pinned, err := db.GetRCGate(second.ID)
	if err != nil {
		t.Fatalf("GetRCGate() error = %v", err)
	}
	for _, source := range pinned.Sources() {
		ready, err := manager.rcGateSideReady(*pinned, source)
		if err != nil || !ready {
			t.Fatalf("rcGateSideReady(%s) = %v, %v; want ready with only the large-db worker", source, ready, err)
		}
		workers, err := db.ListWorkersFiltered("", source)
		if err != nil || len(workers) != 1 || workers[0].ProfileName != gateLargeDBProfile {
			t.Fatalf("%s workers = %+v, %v; want only gate-large-db", source, workers, err)
		}
	}
	for _, source := range first.Sources() {
		if workers, err := db.ListWorkersFiltered("", source); err != nil || len(workers) != 0 {
			t.Fatalf("legacy gate source %s gained workers from the rerun: %+v, %v", source, workers, err)
		}
		ready, err := manager.rcGateSideReady(first, source)
		if err != nil || ready {
			t.Fatalf("rcGateSideReady(%s) = %v, %v on the legacy gate, want not ready", source, ready, err)
		}
	}
}

func TestIsGateSourceAcceptsPerGateSuffix(t *testing.T) {
	tests := []struct {
		source string
		want   bool
	}{
		{source: "rc-a25ac011", want: true},
		{source: "rcbase-9f3af35a", want: true},
		{source: "rc-a25ac011-2dfdc1c3", want: true},
		{source: "rcbase-9f3af35a-2dfdc1c3", want: true},
		{source: "up-a25ac011-r2", want: true},
		{source: "upbase-9f3af35a-1", want: true},
		{source: "rc-a25ac011-2DFDC1C3", want: false},
		{source: "rc-a25ac011-123456789", want: false},
		{source: "rc-a25ac011-", want: false},
		{source: "rc-a25ac0", want: false},
		{source: "pr-1514", want: false},
		{source: "main", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			if got := isGateSource(tt.source); got != tt.want {
				t.Fatalf("isGateSource(%q) = %v, want %v", tt.source, got, tt.want)
			}
		})
	}
}

func TestReadyDeploymentTargetAcceptsSuffixedGateImageLabels(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		label   string
		wantErr string
	}{
		{name: "legacy label", source: "rc-aaaaaaaa", label: "rc-aaaaaaaa"},
		{name: "suffixed label", source: "rc-aaaaaaaa-2dfdc1c3", label: "rc-aaaaaaaa-2dfdc1c3"},
		{name: "suffixed baseline label", source: "rcbase-bbbbbbbb-2dfdc1c3", label: "rcbase-bbbbbbbb-2dfdc1c3"},
		{name: "legacy source refuses a suffixed image", source: "rc-aaaaaaaa", label: "rc-aaaaaaaa-2dfdc1c3", wantErr: "does not match requested source"},
		{name: "suffixed source refuses the legacy image", source: "rc-aaaaaaaa-2dfdc1c3", label: "rc-aaaaaaaa", wantErr: "does not match requested source"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReadyDeploymentTarget(model.Deployment{
				Source:        tt.source,
				Status:        "ready",
				GitSHA:        testGateSoakSHA,
				LitestreamSHA: testGateCandidateSHA,
				ImageRef:      testGateImage(tt.label, testGateCandidateSHA),
			})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateReadyDeploymentTarget() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateReadyDeploymentTarget() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
