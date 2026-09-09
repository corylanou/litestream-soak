package model

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestEvidenceWindowPreservesMatchingHistory(t *testing.T) {
	db := seriesTestDB(t)
	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	worker := Worker{ID: "window", Name: "window", Source: "main", ProfileConfig: "{}"}
	if err := db.CreateWorker(&worker); err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct {
		deployment int
		at         time.Time
		source     string
	}{
		{7, start.Add(-time.Hour), "main"}, {7, end.Add(time.Hour), "main"},
		{0, start, "main"}, {0, end, ""}, {0, start.Add(-time.Second), "main"}, {0, end.Add(time.Second), "main"},
		{8, start, "main"}, {7, start, "other"},
	} {
		identity := reporting.WorkerIdentity{WorkerID: worker.ID, DeploymentID: row.deployment, Source: row.source, RunID: fmt.Sprint(i)}
		v := Verification{WorkerID: worker.ID, StartedAt: row.at, CompletedAt: &row.at, Status: "failed", CheckType: "integrity", Run: identity, ErrorMessage: "recovered failure"}
		if err := db.RecordVerification(&v); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(identity)
		if err := db.RecordEventAt(worker.ID, "provider_retry", "original failure", string(body), row.at); err != nil {
			t.Fatal(err)
		}
		if err := db.RecordRuntimeEvidence(identity, json.RawMessage(`{}`), false); err != nil {
			t.Fatal(err)
		}
		if _, err := db.writer.Exec(`UPDATE evidence_runtime SET received_at=? WHERE id=?`, row.at, i+1); err != nil {
			t.Fatal(err)
		}
	}
	window := EvidenceWindow{DeploymentID: 7, Start: start, End: &end}
	vs, err := db.ListEvidenceVerifications("main", window)
	if err != nil || len(vs) != 4 {
		t.Fatalf("verifications=%d %v", len(vs), err)
	}
	es, err := db.ListEvidenceEvents("main", window)
	if err != nil || len(es) != 4 {
		t.Fatalf("events=%d %v", len(es), err)
	}
	rs, err := db.ListRuntimeEvidence("main", 7, window)
	if err != nil || len(rs) != 4 {
		t.Fatalf("runtime=%d %v", len(rs), err)
	}
	window.End = nil
	vs, err = db.ListEvidenceVerifications("main", window)
	if err != nil || len(vs) != 5 {
		t.Fatalf("active verifications=%d %v", len(vs), err)
	}
	for _, v := range vs {
		if v.HistoryComplete || v.Succeeded() {
			t.Fatalf("false clean: %+v", v)
		}
	}
	for _, tt := range []struct{ table, dep, at, index string }{
		{"evidence_verifications", verificationDeployment, "COALESCE(completed_at,started_at)", "evidence_verification_window"},
		{"evidence_events", eventDeployment, "created_at", "evidence_event_window"},
	} {
		query, args := evidenceWindowQuery(tt.table, "*", tt.dep, tt.at, "id", "main", []EvidenceWindow{window})
		rows, err := db.reader.Query("EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "\n")
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		t.Log(plan.String())
		if strings.Count(plan.String(), "USING INDEX "+tt.index) != 2 || strings.Contains(plan.String(), "SCAN "+tt.table) {
			t.Fatalf("unbounded plan: %s", plan.String())
		}
	}
}

func TestEvidenceReadCancellationReleasesPool(t *testing.T) {
	db := seriesTestDB(t)
	db.reader.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	scoped := db.WithReadContext(ctx)
	done := make(chan error, 1)
	go func() {
		rows, err := scoped.query(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n`)
		if rows != nil {
			_ = rows.Close()
		}
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled expensive query succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query ignored cancellation")
	}
	healthCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := db.HealthCheck(healthCtx); err != nil {
		t.Fatal(err)
	}
}
