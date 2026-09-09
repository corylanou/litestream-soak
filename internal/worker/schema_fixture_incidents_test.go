package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestSchemaFixtureProcessIncidents(t *testing.T) {
	log := "time=2026-09-09T12:00:00Z level=INFO msg=litestream version=pinned\n" +
		"time=2026-09-09T12:00:01Z level=ERROR msg=\"replication failed\" error=temporary\n" +
		"time=2026-09-09T12:00:02Z level=INFO msg=\"retrying upload\"\n" +
		"time=2026-09-09T12:00:03Z level=INFO msg=\"snapshot complete\"\n"
	evidence, err := readSchemaProcessEvidence(strings.NewReader(log))
	if err != nil || evidence.Status != "parsed" || evidence.IncidentCount != 2 || len(evidence.Incidents) != 2 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
	if got := schemaProcessVerdict("scenario_success", evidence); got != "recovered_with_incidents" {
		t.Fatalf("verdict=%s", got)
	}
	if got := schemaProcessVerdict("unrelated_failure", evidence); got != "unrelated_failure" {
		t.Fatalf("failure overwritten: %s", got)
	}
}

func TestSchemaFixtureUnknownProcessLogs(t *testing.T) {
	for _, log := range []string{"", "unknown log format\n", "time=now level=INFO msg=\"unterminated\n"} {
		evidence, err := readSchemaProcessEvidence(strings.NewReader(log))
		if err != nil {
			t.Fatal(err)
		}
		if got := schemaProcessVerdict("scenario_success", evidence); got != "inconclusive" {
			t.Fatalf("unknown log passed: %+v", evidence)
		}
	}
}

func TestSchemaFixtureCleanProcessLogs(t *testing.T) {
	evidence, err := readSchemaProcessEvidence(strings.NewReader("time=now level=INFO msg=\"snapshot complete\"\n"))
	if err != nil || schemaProcessVerdict("scenario_success", evidence) != "scenario_success" {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
}

type schemaSyncBudgetTransport struct{ t *testing.T }

func (transport schemaSyncBudgetTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body struct {
		Timeout int `json:"timeout"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		transport.t.Fatal(err)
	}
	if body.Timeout != 30 {
		transport.t.Errorf("sync timeout=%d, want full 30-second boundary budget", body.Timeout)
	}
	return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("injected sync error")), Header: make(http.Header)}, nil
}

func TestSchemaFixtureSyncUsesBoundaryBudget(t *testing.T) {
	v := NewVerifier(DefaultConfig())
	v.httpClient = &http.Client{Transport: schemaSyncBudgetTransport{t: t}}
	boundary := SchemaFixtureBoundary{}
	err := verifySchemaFixture(context.Background(), v, "unused", schemaFixtureStep{name: "growth"}, t.TempDir(), &boundary)
	if err == nil || len(boundary.SyncAttempts) != 1 || boundary.SyncAttempts[0].Status != "failed" {
		t.Fatalf("err=%v boundary=%+v", err, boundary)
	}
}

func TestSchemaFixtureStartupUnavailable(t *testing.T) {
	for _, tc := range []struct {
		step string
		err  error
		want bool
	}{
		{"initialize", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true},
		{"initialize", fmt.Errorf("dial: %w", os.ErrNotExist), true},
		{"growth", syscall.ECONNREFUSED, false},
		{"initialize", fmt.Errorf("sync returned 500"), false},
		{"initialize", nil, false},
	} {
		if got := schemaStartupUnavailable(tc.step, tc.err); got != tc.want {
			t.Fatalf("step=%s err=%v got=%v", tc.step, tc.err, got)
		}
	}
}
