package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTenantLifecycleGenerationAndFairRetry(t *testing.T) {
	now := time.Unix(100, 0)
	q := newTenantLedger(2)
	a, err := q.create(0, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.create(1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.create(0, now); err == nil {
		t.Fatal("duplicate live tenant accepted")
	}
	q.attempt(a, "restore mismatch", now.Add(time.Second))
	if got := q.pending()[0]; got != b {
		t.Fatalf("failed tenant starved peer: %v", got)
	}
	q.acknowledge(a, now.Add(2*time.Second))
	if err := q.retire(a, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	next, err := q.create(0, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if a == next || a.name() == next.name() {
		t.Fatal("generation reused replica prefix")
	}
	q.acknowledge(a, now.Add(5*time.Second))
	pending := q.pending()
	if len(pending) != 2 || !slices.Contains(pending, next) {
		t.Fatalf("stale acknowledgement cleared new generation: %v", pending)
	}
	if len(q.failures) != 1 {
		t.Fatal("self-healing erased failure")
	}
	if _, err := q.create(2, now); err == nil {
		t.Fatal("tenant bound exceeded")
	}
	if got := q.oldestAge(now.Add(10 * time.Second)); got != 10 {
		t.Fatalf("coverage age=%v", got)
	}
}

func TestTenantLifecycleRequiresVerifiedRetirement(t *testing.T) {
	q := newTenantLedger(1)
	id, err := q.create(0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.retire(id, time.Now()); err == nil {
		t.Fatal("unverified backup retired")
	}
	q.acknowledge(id, time.Now())
	if err := q.retire(id, time.Now()); err != nil {
		t.Fatal(err)
	}
	second, err := q.create(0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	q.acknowledge(second, time.Now())
	if err := q.retire(second, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := q.create(0, time.Now()); err == nil {
		t.Fatal("retained generation bound exceeded")
	}
}

func TestTenantLifecycleOptionsRejectUnsafeInputs(t *testing.T) {
	for _, options := range []TenantLifecycleOptions{
		{Mode: "unknown", Tenants: 100},
		{Mode: "watch", Tenants: 0},
		{Mode: "watch", Tenants: 1001},
	} {
		if err := options.validate(); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
	for _, count := range []int{2, 100, 500, 1000} {
		if err := (TenantLifecycleOptions{Mode: "static", Tenants: count, SHA: tenantLitestreamSHA, Capabilities: "directory-v1", Binary: "litestream", Root: t.TempDir(), SyncTimeout: time.Second, Timeout: time.Minute}).validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTenantLifecycleCandidatePin(t *testing.T) {
	for _, sha := range []string{tenantLitestreamSHA, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"} {
		options := TenantLifecycleOptions{Binary: "litestream", Root: t.TempDir(), Mode: "watch", Tenants: 100, SyncTimeout: time.Second, Timeout: time.Minute, SHA: sha, Capabilities: "directory-v1"}
		if err := options.validate(); err != nil {
			t.Fatal(err)
		}
		options.SHA = "main"
		if err := options.validate(); err == nil {
			t.Fatal("moving ref accepted")
		}
		options.SHA = sha
		options.Capabilities = "unknown"
		if err := options.validate(); err == nil {
			t.Fatal("unknown capability silently accepted")
		}
	}
}

func TestTenantLifecycleRetryEvidence(t *testing.T) {
	report := TenantLifecycleReport{}
	r := tenantRunner{options: TenantLifecycleOptions{Timeout: time.Second}, report: &report}
	attempts := 0
	err := r.wait(context.Background(), "discovery", tenantID{1, 1}, func(context.Context) (bool, error) {
		attempts++
		if attempts == 1 {
			return false, errors.New("temporary IPC failure")
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Failures) != 1 {
		t.Fatal("recovered IPC failure was not retained")
	}
	if len(report.Attempts) != 2 || report.Attempts[0].Detail != "temporary IPC failure" || report.Attempts[0].Tenant != "tenant-00001-g01.db" {
		t.Fatalf("lost retry evidence: %+v", report.Attempts)
	}
}

func TestTenantLifecycleFullLogPreservesEarlyFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "process.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	log := tenantProcessLog{file: file, remaining: 1 << 20}
	if _, err := log.Write([]byte("level=ERROR early failure\n" + strings.Repeat("level=INFO healthy\n", 200))); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report := TenantLifecycleReport{ProcessLog: path}
	r := tenantRunner{report: &report}
	if err := r.reviewLog(); err == nil {
		t.Fatal("self-healed error passed")
	}
	if len(report.Failures) != 1 {
		t.Fatalf("failures=%+v", report.Failures)
	}
	log.remaining = 0
	if _, err := log.Write([]byte("x")); err == nil {
		t.Fatal("log bound ignored")
	}
}

func TestTenantLifecycleBatchRetainsEveryFailure(t *testing.T) {
	q := newTenantLedger(2)
	for i := 0; i < 2; i++ {
		id, err := q.create(i, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		q.acknowledge(id, time.Now())
		if err := q.retire(id, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	report := TenantLifecycleReport{}
	cfg := DefaultConfig()
	r := tenantRunner{options: TenantLifecycleOptions{Binary: filepath.Join(dir, "missing-binary"), Timeout: time.Second}, report: &report, ledger: q, dir: dir, log: newLineBuffer(10), fullLog: &tenantProcessLog{file: file, remaining: 1 << 20}, verifier: NewVerifier(cfg), expected: make(map[tenantID]logicalSnapshot)}
	if err := r.verifyPending(context.Background(), "retained"); err == nil {
		t.Fatal("failed restores passed")
	}
	if len(report.Failures) != 2 || len(q.pending()) != 2 {
		t.Fatalf("lost peer failure or pending work: %+v %+v", report, q.pending())
	}
	for _, id := range q.pending() {
		q.acknowledge(id, time.Now())
	}
	if len(report.Failures) != 2 {
		t.Fatal("later acknowledgements erased failure history")
	}
}

func TestTenantLifecycleSyncUsesConfiguredRequestBudget(t *testing.T) {
	dir := t.TempDir()
	id := tenantID{0, 1}
	report := TenantLifecycleReport{}
	cfg := DefaultConfig()
	q := newTenantLedger(2)
	r := tenantRunner{options: TenantLifecycleOptions{Binary: filepath.Join(dir, "missing"), SyncTimeout: 5 * time.Second, Timeout: 5 * time.Second}, dir: dir, report: &report, ledger: q, verifier: NewVerifier(cfg), expected: make(map[tenantID]logicalSnapshot), log: newLineBuffer(10)}
	if err := os.Mkdir(filepath.Join(dir, "dbs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.create(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	var budget int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/list" {
			_, _ = fmt.Fprintf(w, `{"databases":[{"path":%q}]}`, r.path(id))
			return
		}
		var input struct {
			Timeout int `json:"timeout"`
		}
		if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		budget = input.Timeout
		_, _ = io.WriteString(w, `{"txid":1,"replicated_txid":1}`)
	}))
	defer server.Close()
	r.verifier.httpClient = &http.Client{Transport: tenantTestTransport{base: server.URL}}
	if err := r.verify(context.Background(), id); err == nil {
		t.Fatal("missing restore binary passed")
	}
	if budget < 4 {
		t.Fatalf("sync budget=%ds, operation budget=5s", budget)
	}
}

type tenantTestTransport struct{ base string }

func (tr tenantTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	target, err := url.Parse(tr.base + req.URL.Path)
	if err != nil {
		return nil, err
	}
	copy.URL = target
	return http.DefaultTransport.RoundTrip(copy)
}
