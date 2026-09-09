package recovery

import "testing"

func TestBoundaryLoss(t *testing.T) {
	for _, tt := range []struct {
		name                                              string
		committed, confirmed, restored, async, replicated int
		valid                                             bool
	}{
		{"complete", 10, 8, 10, 0, 0, true}, {"async tail", 10, 8, 8, 2, 0, true}, {"replicated loss", 10, 8, 6, 2, 2, true},
		{"impossible confirmed", 5, 6, 5, 0, 0, false}, {"unexpected rows", 5, 4, 6, 0, 0, false}, {"negative", -1, 0, 0, 0, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MeasureBoundary(tt.committed, tt.confirmed, tt.restored)
			if (err == nil) != tt.valid {
				t.Fatalf("boundary=%+v err=%v", b, err)
			}
			if tt.valid && (b.AsyncLoss != tt.async || b.ReplicatedLoss != tt.replicated) {
				t.Fatalf("boundary=%+v", b)
			}
		})
	}
}

func TestVerdictPreservesFailuresAndExposure(t *testing.T) {
	for _, tt := range []struct {
		name     string
		attempts []Attempt
		want     string
	}{
		{"empty", nil, "inconclusive"},
		{"fault without recovery", []Attempt{{Expected: true, Engaged: true, Error: "signal: killed"}}, "inconclusive"},
		{"unengaged", []Attempt{{Oracle: true}}, "inconclusive"},
		{"valid", []Attempt{{Oracle: true, Engaged: true}}, "passed"},
		{"self heal", []Attempt{{Error: "failed"}, {Oracle: true, Engaged: true}}, "failed"},
		{"replicated loss", []Attempt{{Oracle: true, Engaged: true, Boundary: Boundary{ReplicatedLoss: 1}}}, "failed"},
		{"expected fault", []Attempt{{Error: "signal: killed", Expected: true, Engaged: true}, {Oracle: true, Engaged: true}}, "passed"},
		{"unexecuted", []Attempt{{Status: "unexecuted"}, {Oracle: true, Engaged: true}}, "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := Verdict(tt.attempts); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestCapabilitiesRequireExplicitEvidence(t *testing.T) {
	c := DetectCapabilities("pin", "-timestamp TIMESTAMP\n-f\n-follow-interval DURATION")
	if !c.Timestamp || !c.Follow || c.Pin != "pin" || c.Evidence == "" {
		t.Fatalf("%+v", c)
	}
	c = DetectCapabilities("old", "-force\n-file")
	if c.Timestamp || c.Follow {
		t.Fatalf("false support: %+v", c)
	}
}

func TestObservationsCannotManufactureExposure(t *testing.T) {
	if got := Verdict([]Attempt{{Status: "observation", Oracle: true}}); got != "inconclusive" {
		t.Fatal(got)
	}
	if got := Verdict([]Attempt{{Status: "observation", Oracle: true}, {Engaged: true, Oracle: true}}); got != "passed" {
		t.Fatal(got)
	}
	if got := Verdict([]Attempt{{Status: "observation", Error: "retry failed"}, {Engaged: true, Oracle: true}}); got != "failed" {
		t.Fatal(got)
	}
}

func TestHistoricalTargetsDoNotReportAsyncLoss(t *testing.T) {
	a := Attempt{Boundary: Boundary{Committed: 516, Confirmed: 512, Restored: 512, AsyncLoss: 4}}
	a.historical(512, false)
	if a.Boundary.AsyncLoss != 0 || a.Boundary.ReplicatedLoss != 0 || a.Boundary.Scope != "historical-target" {
		t.Fatalf("%+v", a)
	}
	a = Attempt{Boundary: Boundary{Committed: 516, Confirmed: 512, Restored: 0, ReplicatedLoss: 512}}
	a.historical(512, true)
	if a.Boundary.ReplicatedLoss != 0 || a.Boundary.Scope != "expired-target" {
		t.Fatalf("%+v", a)
	}
}
