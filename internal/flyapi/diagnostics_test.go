package flyapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDiagnosticMachinePreservesOperationalConfig(t *testing.T) {
	machine := &Machine{ID: "machine", InstanceID: "instance", CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(2, 0), Config: MachineConfig{Image: "image", Env: map[string]string{"FUTURE_CREDENTIAL": "synthetic-secret"}, Guest: Guest{CPUs: 2}, Mounts: []Mount{{Volume: "volume", Path: "/data"}}, Metrics: &MetricsConfig{Port: 9091}, Services: []Service{{InternalPort: 8080}}}, Events: []MachineEvent{{Type: "start"}}}
	before, err := json.Marshal(machine)
	if err != nil {
		t.Fatal("marshal internal machine")
	}
	diagnostic := machine.Diagnostic()
	body, err := json.Marshal(diagnostic)
	if err != nil {
		t.Fatal("marshal diagnostic")
	}
	for _, forbidden := range []string{"synthetic-secret", "FUTURE_CREDENTIAL", `"env"`, `"services"`, `"metrics"`} {
		if strings.Contains(string(body), forbidden) {
			t.Error("unsafe machine field exposed")
		}
	}
	if diagnostic.InstanceID != machine.InstanceID || diagnostic.CreatedAt != machine.CreatedAt || diagnostic.UpdatedAt != machine.UpdatedAt || diagnostic.Config.Guest.CPUs != 2 {
		t.Error("safe identity or resource metadata missing")
	}
	diagnostic.Config.Mounts[0].Volume = "changed"
	diagnostic.Events[0].Type = "changed"
	after, err := json.Marshal(machine)
	if err != nil {
		t.Fatal("marshal internal machine after conversion")
	}
	if string(before) != string(after) {
		t.Error("diagnostic conversion mutated operational machine")
	}
	request, err := json.Marshal(CreateMachineRequest{Config: machine.Config})
	if err != nil {
		t.Fatal("marshal operational request")
	}
	if !strings.Contains(string(request), "synthetic-secret") {
		t.Error("operational credentials lost")
	}
	var absent *Machine
	if absent.Diagnostic() != nil {
		t.Error("nil machine produced diagnostic")
	}
}

func TestDiagnosticErrorExcludesProviderBody(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{nil, ""},
		{errors.New("synthetic-secret"), "Machine metadata unavailable"},
		{fmt.Errorf("wrapped: %w", &APIError{StatusCode: 503, Body: "synthetic-secret"}), "Machine metadata unavailable (provider HTTP 503)"},
	} {
		if got := DiagnosticError(test.err); got != test.want {
			t.Error("unexpected public diagnostic error")
		}
	}
}

func TestAPIErrorPublicFormattingExcludesBody(t *testing.T) {
	err := &APIError{StatusCode: 502, Body: "sentinel-private-body"}
	for _, formatted := range []string{err.Error(), fmt.Sprint(err), fmt.Sprintf("%+v", err)} {
		if strings.Contains(formatted, "sentinel-private-body") {
			t.Error("error formatting exposed provider body")
		}
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatal("marshal provider error")
	}
	if strings.Contains(string(encoded), "sentinel-private-body") {
		t.Error("error JSON exposed provider body")
	}
	if err.Body != "sentinel-private-body" {
		t.Error("internal provider evidence lost")
	}
}
