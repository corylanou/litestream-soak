package model

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

func TestVerificationActivityJournalLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activity.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	identity := reporting.WorkerIdentity{WorkerID: "worker", RunID: "run", MachineID: "machine"}
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	event := struct {
		reporting.WorkerEventPayload
		Attributed bool `json:"attributed"`
	}{reporting.WorkerEventPayload{WorkerIdentity: identity, ActiveVerification: &reporting.ActiveVerification{StartedAt: at, CheckType: "integrity"}}, true}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordEventAt("worker", "verification_started", "start", string(raw), at); err != nil {
		t.Fatal(err)
	}
	end := at.Add(time.Minute)
	if err := db.RecordVerification(&Verification{WorkerID: "worker", Run: identity, Attributed: true, StartedAt: at, CompletedAt: &end, Status: "failed", CheckType: "integrity"}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []struct {
		sql, index string
		args       []any
	}{
		{latestRunVerificationStartQuery, "evidence_activity_start", []any{"worker", "run", "machine"}},
		{runVerificationCompletionQuery, "evidence_activity_completion", []any{"worker", "run", "machine", at, "integrity"}},
	} {
		rows, err := db.reader.Query("EXPLAIN QUERY PLAN "+query.sql, query.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var a, b, c int
			var detail string
			if err := rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if !strings.Contains(plan.String(), "USING INDEX "+query.index) || strings.Contains(plan.String(), "SCAN ") || strings.Contains(plan.String(), "TEMP B-TREE") {
			t.Fatalf("unbounded plan: %s", plan.String())
		}
	}
	if _, err := db.PruneEventsBefore(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PruneVerificationsBefore(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	start, err := db.LatestRunVerificationStart(identity)
	if err != nil || start == nil || start.Details != string(raw) {
		t.Fatalf("start not retained: %v %v", start, err)
	}
	completion, err := db.RunVerificationCompletion(identity, at, "integrity")
	if err != nil || completion == nil || completion.Status != "failed" {
		t.Fatalf("completion not retained: %v %v", completion, err)
	}
	other := identity
	other.RunID = "other"
	if start, err := db.LatestRunVerificationStart(other); err != nil || start != nil {
		t.Fatalf("unrelated start: %v %v", start, err)
	}
	for _, query := range []struct {
		identity reporting.WorkerIdentity
		at       time.Time
		check    string
	}{{other, at, "integrity"}, {identity, at.Add(time.Second), "integrity"}, {identity, at, "other"}} {
		if result, err := db.RunVerificationCompletion(query.identity, query.at, query.check); err != nil || result != nil {
			t.Fatalf("unrelated completion: %v %v", result, err)
		}
	}
}
