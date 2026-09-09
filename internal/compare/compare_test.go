package compare

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
)

func fixturePlan() Plan {
	return Plan{ID: "experiment-1", Baseline: "main", Candidate: "branch:fix", Repeats: 4, Contract: Contract{Backend: "file", FixtureSHA256: strings.Repeat("a", 64), FixtureBytes: 1024, FixtureAgeSeconds: 3600, Seed: 42, GeneratorSHA: strings.Repeat("b", 40), OracleSHA: strings.Repeat("c", 40), Hardware: "shared-cpu-1x/1024", Region: "ord", ConfigSHA256: strings.Repeat("d", 64), OperationBudget: 100, Toolchain: "go1.25.13"}}
}

func pinnedPlan(t *testing.T) Plan {
	t.Helper()
	p, err := Pin(context.Background(), fixturePlan(), func(_ context.Context, ref string) (string, error) {
		if ref == "branch:fix" {
			return strings.Repeat("e", 40), nil
		}
		return strings.Repeat("f", 40), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPinResolvesEachMovingReferenceOnce(t *testing.T) {
	p := fixturePlan()
	p.Candidate = "main"
	calls := map[string]int{}
	p, err := Pin(context.Background(), p, func(_ context.Context, ref string) (string, error) { calls[ref]++; return strings.Repeat("f", 40), nil })
	if err != nil {
		t.Fatal(err)
	}
	if calls["main"] != 1 || p.MainSHA != p.Baseline || p.Baseline != p.Candidate {
		t.Fatalf("moving target: %+v calls=%v", p, calls)
	}
}

func TestPinRejectsInvalidIdentity(t *testing.T) {
	for _, sha := range []string{"", "abcd123", strings.Repeat("x", 40)} {
		t.Run(sha, func(t *testing.T) {
			_, err := Pin(context.Background(), fixturePlan(), func(context.Context, string) (string, error) { return sha, nil })
			if err == nil {
				t.Fatal("accepted invalid SHA")
			}
		})
	}
	_, err := Pin(context.Background(), fixturePlan(), func(context.Context, string) (string, error) { return "", errors.New("offline") })
	if err == nil {
		t.Fatal("lost resolver error")
	}
}

func TestPlanRejectsIncompleteOrUnsafeContracts(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Plan)
	}{
		{"traversal", func(p *Plan) { p.ID = "../escape" }},
		{"one pair", func(p *Plan) { p.Repeats = 1 }},
		{"no budget", func(p *Plan) { p.Contract.OperationBudget = 0 }},
		{"unknown fixture", func(p *Plan) { p.Contract.FixtureSHA256 = "" }},
		{"unknown generator", func(p *Plan) { p.Contract.GeneratorSHA = "main" }},
		{"unknown oracle", func(p *Plan) { p.Contract.OracleSHA = "" }},
		{"unknown hardware", func(p *Plan) { p.Contract.Hardware = "" }},
		{"unknown region", func(p *Plan) { p.Contract.Region = "" }},
		{"unknown config", func(p *Plan) { p.Contract.ConfigSHA256 = "" }},
		{"unknown compiler", func(p *Plan) { p.Contract.Toolchain = "" }},
		{"negative age", func(p *Plan) { p.Contract.FixtureAgeSeconds = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := pinnedPlan(t)
			tt.change(&p)
			if _, err := Schedule(p); err == nil {
				t.Fatal("accepted invalid contract")
			}
		})
	}
}

func TestScheduleMatchesPairsAndIsolatesAllReplicas(t *testing.T) {
	p := pinnedPlan(t)
	runs, err := Schedule(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 4*4*2*2 {
		t.Fatalf("runs=%d", len(runs))
	}
	prefixes := map[string]bool{}
	for i := 0; i < len(runs); i += 2 {
		a, b := runs[i], runs[i+1]
		if a.Contract != b.Contract || a.Contract != p.Contract || a.Scenario != b.Scenario || a.Pair != b.Pair {
			t.Fatal("unmatched pair")
		}
		if a.Arm == b.Arm || (a.Pair%2 == 0 && a.Arm != "baseline") || (a.Pair%2 == 1 && a.Arm != "candidate") {
			t.Fatal("ordering not counterbalanced")
		}
		for _, r := range []Request{a, b} {
			if prefixes[r.ReplicaPrefix] {
				t.Fatal("replica collision")
			}
			prefixes[r.ReplicaPrefix] = true
			if r.Kind == "calibration" && r.SHA != p.MainSHA {
				t.Fatal("calibration not main/main")
			}
			if r.Scenario == "no-litestream" && r.Litestream {
				t.Fatal("control enables replication")
			}
		}
	}
}

func successful(r Request) Observation {
	v := 10.0
	return Observation{Request: r, CompletedOperations: r.Contract.OperationBudget, Correctness: "pass", Reliability: "pass", Metrics: map[string]Measurement{"cpu_seconds_per_operation": {Value: &v}}}
}

func TestRunPreservesFailuresAndUnknownCapabilities(t *testing.T) {
	p := pinnedPlan(t)
	n := 0
	report, err := Run(context.Background(), p, func(_ context.Context, r Request) (Observation, error) {
		n++
		o := successful(r)
		if n == 1 {
			o.Incidents = []Incident{{Category: "correctness", Detail: "self healed corruption"}}
		}
		if n == 2 {
			return o, errors.New("executor failed")
		}
		return o, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Observations) != 64 || len(report.Observations[0].Incidents) != 1 || report.Observations[1].Error == "" {
		t.Fatal("lost adverse evidence")
	}
	if report.Verdict != "adverse" {
		t.Fatalf("verdict=%s", report.Verdict)
	}
	if report.Observations[0].Metrics["rss_bytes"].Unavailable == "" {
		t.Fatal("missing capability hidden")
	}
}

func TestRunRejectsMismatchedEvidence(t *testing.T) {
	for _, change := range []func(*Observation){func(o *Observation) { o.Request.Contract.Seed++ }, func(o *Observation) { o.CompletedOperations-- }, func(o *Observation) { o.Request.SHA = "main" }, func(o *Observation) { v := math.NaN(); o.Metrics["rss_bytes"] = Measurement{Value: &v} }, func(o *Observation) { o.Correctness = "" }} {
		report, err := Run(context.Background(), pinnedPlan(t), func(_ context.Context, r Request) (Observation, error) { o := successful(r); change(&o); return o, nil })
		if err != nil {
			t.Fatal(err)
		}
		if report.Verdict != "adverse" || report.Observations[0].Error == "" {
			t.Fatal("invalid evidence accepted")
		}
	}
}

func TestRunCancellationRetainsPartialEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	report, err := Run(ctx, pinnedPlan(t), func(_ context.Context, r Request) (Observation, error) { cancel(); return successful(r), nil })
	if !errors.Is(err, context.Canceled) || len(report.Observations) != 1 || report.Verdict != "inconclusive" {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestUncertaintyDoesNotCancelIndependentRegressions(t *testing.T) {
	report, err := Run(context.Background(), pinnedPlan(t), func(_ context.Context, r Request) (Observation, error) {
		o := successful(r)
		cpu, rss := 10.0, 100.0
		if r.Kind == "comparison" && r.Arm == "candidate" {
			cpu = 5
			rss = 200
		}
		o.Metrics["cpu_seconds_per_operation"] = Measurement{Value: &cpu}
		o.Metrics["rss_bytes"] = Measurement{Value: &rss}
		return o, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	foundImprovement, foundRegression, foundUnavailable := false, false, false
	for _, s := range report.Summaries {
		if s.Kind != "comparison" {
			continue
		}
		if s.Metric == "cpu_seconds_per_operation" && s.Verdict == "improvement" {
			foundImprovement = true
		}
		if s.Metric == "rss_bytes" && s.Verdict == "regression" {
			foundRegression = true
		}
		if s.Metric == "restore_seconds" && s.Verdict == "unavailable" {
			foundUnavailable = true
		}
	}
	if !foundImprovement || !foundRegression || !foundUnavailable || report.Verdict != "adverse" {
		t.Fatalf("summaries=%+v", report.Summaries)
	}
}

func TestCalibrationNoiseIsInconclusive(t *testing.T) {
	report, err := Run(context.Background(), pinnedPlan(t), func(_ context.Context, r Request) (Observation, error) {
		o := successful(r)
		v := 10.0
		if r.Arm == "candidate" {
			if r.Kind == "calibration" {
				v += 3
			} else {
				v += 1
			}
		}
		o.Metrics["cpu_seconds_per_operation"] = Measurement{Value: &v}
		return o, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range report.Summaries {
		if s.Kind == "comparison" && s.Metric == "cpu_seconds_per_operation" && (s.Verdict != "inconclusive" || s.NoiseFloor != 3) {
			t.Fatalf("noise hidden: %+v", s)
		}
	}
	if report.Performance != "inconclusive" {
		t.Fatal(report.Performance)
	}
}

func TestPinReusesMainBranchAlias(t *testing.T) {
	p := fixturePlan()
	p.Candidate = "branch:main"
	calls := 0
	pinned, err := Pin(context.Background(), p, func(_ context.Context, ref string) (string, error) {
		calls++
		if calls > 1 {
			return strings.Repeat("e", 40), nil
		}
		return strings.Repeat("f", 40), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || pinned.MainSHA != pinned.Candidate {
		t.Fatalf("main alias moved: calls=%d plan=%+v", calls, pinned)
	}
}

func TestCompletedCleanExperimentHasExplicitCategoryVerdicts(t *testing.T) {
	report, err := Run(context.Background(), pinnedPlan(t), func(_ context.Context, r Request) (Observation, error) { return successful(r), nil })
	if err != nil {
		t.Fatal(err)
	}
	if report.Correctness != "pass" || report.Verdict != "no_adverse_observed" || report.Performance != "inconclusive" {
		t.Fatalf("report verdict=%s correctness=%s performance=%s", report.Verdict, report.Correctness, report.Performance)
	}
}
