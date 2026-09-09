package recovery

import (
	"errors"
	"strings"
	"time"
)

type Boundary struct {
	Scope          string `json:"scope"`
	Committed      int    `json:"committed"`
	Confirmed      int    `json:"confirmed_replicated"`
	Restored       int    `json:"restored"`
	AsyncLoss      int    `json:"async_loss_rows"`
	ReplicatedLoss int    `json:"replicated_loss_rows"`
	Backlog        int    `json:"unconfirmed_rows"`
}

func MeasureBoundary(committed, confirmed, restored int) (Boundary, error) {
	b := Boundary{Scope: "latest-prefix", Committed: committed, Confirmed: confirmed, Restored: restored, Backlog: committed - confirmed}
	if confirmed < 0 || committed < confirmed || restored < 0 || restored > committed {
		return b, errors.New("invalid recovery boundaries")
	}
	b.AsyncLoss = committed - max(confirmed, restored)
	b.ReplicatedLoss = max(0, confirmed-restored)
	return b, nil
}

type Attempt struct {
	ProcessFinished time.Time     `json:"process_finished,omitempty"`
	Name            string        `json:"name"`
	Status          string        `json:"status,omitempty"`
	Started         time.Time     `json:"started"`
	Duration        time.Duration `json:"duration_ns"`
	Engaged         bool          `json:"engaged"`
	Proof           string        `json:"engagement_proof,omitempty"`
	Oracle          bool          `json:"oracle_match"`
	Boundary        Boundary      `json:"boundary"`
	Error           string        `json:"error,omitempty"`
	Expected        bool          `json:"expected_fault,omitempty"`
	Log             string        `json:"log,omitempty"`
}

func Verdict(attempts []Attempt) string {
	status := "passed"
	exposed := false
	recovered := false
	if len(attempts) == 0 {
		return "inconclusive"
	}
	for _, a := range attempts {
		if (a.Error != "" && !a.Expected) || a.Boundary.ReplicatedLoss > 0 {
			return "failed"
		}
		if a.Status == "observation" {
			continue
		}
		exposed = exposed || a.Engaged
		recovered = recovered || (a.Oracle && a.Engaged && !a.Expected)
		if a.Status == "unexecuted" || !a.Engaged || (!a.Oracle && !a.Expected) {
			status = "inconclusive"
		}
	}
	if !exposed || !recovered {
		return "inconclusive"
	}
	return status
}

type Capabilities struct {
	Pin       string `json:"binary_sha256"`
	Timestamp bool   `json:"timestamp"`
	Follow    bool   `json:"follow"`
	Evidence  string `json:"evidence"`
}

func DetectCapabilities(pin, help string) Capabilities {
	c := Capabilities{Pin: pin, Evidence: help}
	for _, line := range strings.Split(help, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		c.Timestamp = c.Timestamp || fields[0] == "-timestamp"
		c.Follow = c.Follow || fields[0] == "-f"
	}
	return c
}

func (a *Attempt) historical(target int, expired bool) {
	restored := a.Boundary.Restored
	a.Boundary, _ = MeasureBoundary(target, target, restored)
	a.Boundary.Scope = "historical-target"
	if expired {
		a.Boundary = Boundary{Scope: "expired-target", Committed: target, Confirmed: target}
	}
}
