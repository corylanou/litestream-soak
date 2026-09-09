package compare

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/orchestrator"
	"github.com/corylanou/litestream-soak/internal/reporting"
)

type Contract struct {
	Backend           string `json:"backend"`
	ReplicaEndpoint   string `json:"replica_endpoint,omitempty"`
	ReplicaBucket     string `json:"replica_bucket,omitempty"`
	ReplicaRegion     string `json:"replica_region,omitempty"`
	FixtureSHA256     string `json:"fixture_sha256"`
	FixtureBytes      int64  `json:"fixture_bytes"`
	FixtureAgeSeconds int64  `json:"fixture_age_seconds"`
	Seed              int64  `json:"seed"`
	GeneratorSHA      string `json:"generator_sha"`
	OracleSHA         string `json:"oracle_sha"`
	Hardware          string `json:"hardware"`
	Region            string `json:"region"`
	ConfigSHA256      string `json:"config_sha256"`
	OperationBudget   int64  `json:"operation_budget"`
	Toolchain         string `json:"toolchain"`
}

type Plan struct {
	ID        string   `json:"id"`
	Baseline  string   `json:"baseline"`
	Candidate string   `json:"candidate"`
	MainSHA   string   `json:"main_sha"`
	Repeats   int      `json:"repeats"`
	Contract  Contract `json:"contract"`
}

type Request struct {
	Experiment    string   `json:"experiment"`
	Kind          string   `json:"kind"`
	Scenario      string   `json:"scenario"`
	Pair          int      `json:"pair"`
	Arm           string   `json:"arm"`
	SHA           string   `json:"sha"`
	Litestream    bool     `json:"litestream"`
	ReplicaPrefix string   `json:"replica_prefix"`
	Contract      Contract `json:"contract"`
}

type Measurement struct {
	Value       *float64 `json:"value,omitempty"`
	Unavailable string   `json:"unavailable,omitempty"`
}

type Incident struct {
	Category string `json:"category"`
	Detail   string `json:"detail"`
}

type Observation struct {
	StartedAt           time.Time                       `json:"started_at"`
	FinishedAt          time.Time                       `json:"finished_at"`
	VerifiedAt          time.Time                       `json:"verified_at"`
	WorkloadAttempts    uint64                          `json:"workload_attempts"`
	Maintenance         *reporting.MaintenanceEvidence  `json:"maintenance,omitempty"`
	RunEvidence         *orchestrator.WorkerRunEvidence `json:"run_evidence,omitempty"`
	WorkloadSeconds     float64                         `json:"workload_seconds"`
	ResourceSummary     map[string]float64              `json:"resource_summary,omitempty"`
	ProgressSamples     int                             `json:"progress_samples"`
	CapabilityNotes     map[string]string               `json:"capability_notes,omitempty"`
	Request             Request                         `json:"request"`
	CompletedOperations int64                           `json:"completed_operations"`
	Correctness         string                          `json:"correctness"`
	Reliability         string                          `json:"reliability"`
	Incidents           []Incident                      `json:"incidents"`
	Metrics             map[string]Measurement          `json:"metrics"`
	Error               string                          `json:"error,omitempty"`
}

type Summary struct {
	Kind       string  `json:"kind"`
	Scenario   string  `json:"scenario"`
	Metric     string  `json:"metric"`
	Pairs      int     `json:"pairs"`
	MeanDelta  float64 `json:"mean_delta"`
	Lower      float64 `json:"lower_95"`
	Upper      float64 `json:"upper_95"`
	NoiseFloor float64 `json:"calibration_noise_floor"`
	Verdict    string  `json:"verdict"`
	Method     string  `json:"method"`
}

type Report struct {
	Correctness  string        `json:"correctness"`
	Performance  string        `json:"performance"`
	Scope        string        `json:"scope"`
	Plan         Plan          `json:"plan"`
	Observations []Observation `json:"observations"`
	Summaries    []Summary     `json:"summaries"`
	Verdict      string        `json:"verdict"`
}

func Metrics() []string {
	return []string{"cpu_seconds_per_operation", "allocation_bytes_per_operation", "rss_bytes", "latency_p50_seconds", "latency_p95_seconds", "latency_p99_seconds", "lag_seconds", "restore_seconds", "disk_growth_bytes", "fd_growth", "replica_bytes", "transfer_bytes", "replica_sync_age_seconds", "object_requests"}
}

func validHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

func Pin(ctx context.Context, p Plan, resolve func(context.Context, string) (string, error)) (Plan, error) {
	if p.Baseline == "branch:main" {
		p.Baseline = "main"
	}
	if p.Candidate == "branch:main" {
		p.Candidate = "main"
	}
	cache := map[string]string{}
	for _, ref := range []string{"main", p.Baseline, p.Candidate} {
		if _, ok := cache[ref]; ok {
			continue
		}
		sha := ref
		if !validHex(sha, 40) {
			var err error
			sha, err = resolve(ctx, ref)
			if err != nil {
				return Plan{}, fmt.Errorf("resolve %q: %w", ref, err)
			}
		}
		if !validHex(sha, 40) {
			return Plan{}, fmt.Errorf("ref %q did not resolve to a full commit SHA", ref)
		}
		cache[ref] = sha
	}
	p.MainSHA = cache["main"]
	p.Baseline = cache[p.Baseline]
	p.Candidate = cache[p.Candidate]
	_, err := Schedule(p)
	return p, err
}

func Schedule(p Plan) ([]Request, error) {
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$`).MatchString(p.ID) {
		return nil, errors.New("invalid experiment ID")
	}
	if p.Repeats < 2 || p.Repeats > 100 {
		return nil, errors.New("repeats must be between 2 and 100")
	}
	for _, sha := range []string{p.MainSHA, p.Baseline, p.Candidate, p.Contract.GeneratorSHA, p.Contract.OracleSHA} {
		if !validHex(sha, 40) {
			return nil, errors.New("full immutable source SHAs are required")
		}
	}
	c := p.Contract
	if c.Backend != "file" && c.Backend != "s3" {
		return nil, errors.New("backend must be file or s3")
	}
	if c.Backend == "s3" {
		endpoint, err := url.Parse(c.ReplicaEndpoint)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || c.ReplicaBucket == "" || strings.ContainsAny(c.ReplicaBucket, "/\\") || c.ReplicaRegion == "" {
			return nil, errors.New("complete non-secret S3 destination required")
		}
	}
	if !validHex(c.FixtureSHA256, 64) || !validHex(c.ConfigSHA256, 64) || c.FixtureBytes <= 0 || c.FixtureAgeSeconds < 0 || c.OperationBudget <= 0 || c.Hardware == "" || c.Region == "" || c.Toolchain == "" {
		return nil, errors.New("complete fixture, environment, configuration and operation budget required")
	}
	var runs []Request
	for _, kind := range []string{"calibration", "comparison"} {
		for _, scenario := range []string{"favorable", "representative", "saturation", "no-litestream"} {
			for pair := 0; pair < p.Repeats; pair++ {
				arms := []string{"baseline", "candidate"}
				if pair%2 == 1 {
					arms = []string{"candidate", "baseline"}
				}
				for _, arm := range arms {
					sha := p.Baseline
					if arm == "candidate" {
						sha = p.Candidate
					}
					if kind == "calibration" {
						sha = p.MainSHA
					}
					runs = append(runs, Request{Experiment: p.ID, Kind: kind, Scenario: scenario, Pair: pair, Arm: arm, SHA: sha, Litestream: scenario != "no-litestream", ReplicaPrefix: fmt.Sprintf("%s/%s/%s/%03d/%s", p.ID, kind, scenario, pair, arm), Contract: c})
				}
			}
		}
	}
	return runs, nil
}

func Run(ctx context.Context, p Plan, execute func(context.Context, Request) (Observation, error)) (Report, error) {
	report := Report{Plan: p, Verdict: "inconclusive"}
	runs, err := Schedule(p)
	if err != nil {
		return report, err
	}
	for _, r := range runs {
		if err := ctx.Err(); err != nil {
			summarize(&report)
			return report, err
		}
		o, err := execute(ctx, r)
		if err != nil {
			o.Error = errors.Join(errors.New(o.Error), err).Error()
		}
		if o.Request != r {
			o.Error += "; execution identity or contract mismatch"
		}
		if o.CompletedOperations != r.Contract.OperationBudget {
			o.Error += "; operation budget not completed"
		}
		if o.Correctness != "pass" && o.Correctness != "fail" && o.Correctness != "unavailable" {
			o.Error += "; correctness capability not reported"
		}
		if o.Reliability != "pass" && o.Reliability != "fail" && o.Reliability != "unavailable" {
			o.Error += "; reliability capability not reported"
		}
		if o.Metrics == nil {
			o.Metrics = map[string]Measurement{}
		}
		for _, metric := range Metrics() {
			m, ok := o.Metrics[metric]
			if !ok {
				m.Unavailable = "executor does not measure " + metric
			}
			if m.Value != nil && (math.IsNaN(*m.Value) || math.IsInf(*m.Value, 0) || m.Unavailable != "") {
				o.Error += "; invalid measurement " + metric
				m = Measurement{Unavailable: "invalid executor measurement"}
			}
			if m.Value == nil && m.Unavailable == "" {
				m.Unavailable = "executor returned no measurement"
			}
			o.Metrics[metric] = m
		}
		report.Observations = append(report.Observations, o)
	}
	summarize(&report)
	return report, nil
}

func summarize(report *Report) {
	report.Verdict = "inconclusive"
	report.Correctness = "unavailable"
	report.Performance = "inconclusive"
	report.Scope = "bounded matched experiment; complete-run reliability eligibility reported separately"
	if len(report.Observations) == 16*report.Plan.Repeats {
		report.Verdict = "no_adverse_observed"
		report.Correctness = "pass"
	}
	for _, o := range report.Observations {
		if o.Correctness == "fail" {
			report.Correctness = "fail"
		} else if o.Correctness != "pass" && report.Correctness != "fail" {
			report.Correctness = "unavailable"
		}
		if o.Error != "" || o.Correctness == "fail" || o.Reliability == "fail" || len(o.Incidents) > 0 {
			report.Verdict = "adverse"
		}
	}
	noise := map[string]float64{}
	for _, kind := range []string{"calibration", "comparison"} {
		for _, scenario := range []string{"favorable", "representative", "saturation", "no-litestream"} {
			for _, metric := range Metrics() {
				s := Summary{Kind: kind, Scenario: scenario, Metric: metric, Verdict: "unavailable", Method: "paired mean delta; two-sided Student t 95% interval; calibration max absolute delta floor"}
				var deltas []float64
				for pair := 0; pair < report.Plan.Repeats; pair++ {
					values := map[string]float64{}
					for _, o := range report.Observations {
						if o.Request.Kind != kind || o.Request.Scenario != scenario || o.Request.Pair != pair || o.Error != "" {
							continue
						}
						m := o.Metrics[metric]
						if m.Value != nil {
							values[o.Request.Arm] = *m.Value
						}
					}
					a, aok := values["baseline"]
					b, bok := values["candidate"]
					if aok && bok {
						deltas = append(deltas, b-a)
					}
				}
				s.Pairs = len(deltas)
				if len(deltas) > 0 {
					for _, d := range deltas {
						s.MeanDelta += d
					}
					s.MeanDelta /= float64(len(deltas))
					s.Verdict = "inconclusive"
					if len(deltas) >= 2 {
						variance := 0.0
						for _, d := range deltas {
							variance += (d - s.MeanDelta) * (d - s.MeanDelta)
						}
						half := tCritical(len(deltas)-1) * math.Sqrt(variance/float64(len(deltas)-1)/float64(len(deltas)))
						s.Lower = s.MeanDelta - half
						s.Upper = s.MeanDelta + half
						key := scenario + "/" + metric
						if kind == "calibration" && len(deltas) == report.Plan.Repeats {
							for _, d := range deltas {
								noise[key] = math.Max(noise[key], math.Abs(d))
							}
						} else if floor, ok := noise[key]; ok && len(deltas) == report.Plan.Repeats {
							s.NoiseFloor = floor
							if s.Lower > floor {
								s.Verdict = "regression"
								report.Performance = "regression"
								report.Verdict = "adverse"
							} else if s.Upper < -floor {
								s.Verdict = "improvement"
								if report.Performance != "regression" {
									report.Performance = "improvements_observed"
								}
							}
						}
					}
				}
				report.Summaries = append(report.Summaries, s)
			}
		}
	}
}

func tCritical(df int) float64 {
	values := []float64{0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042}
	if df < len(values) {
		return values[df]
	}
	return 2.042
}
