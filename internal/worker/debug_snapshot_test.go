package worker

import "testing"

func TestSelectRestorePlanCandidatesCoversTarget(t *testing.T) {
	candidates := parseRestorePlanCandidates(`
level min_txid max_txid size created
9 0000000000000001 00000000000210ff 197156405 2026-05-11T13:33:00Z
3 0000000000021100 00000000000211ff 5159392 2026-05-11T13:00:00Z
2 0000000000021200 0000000000021280 460226 2026-05-11T13:35:00Z
0 0000000000021281 0000000000021289 3796 2026-05-11T13:40:00Z
0 000000000002128a 000000000002128a 207 2026-05-11T13:41:00Z
`)

	targetTXID := uint64(0x21289)
	selected := selectRestorePlanCandidates(candidates, targetTXID)
	if len(selected) != 4 {
		t.Fatalf("selected entries=%d want 4", len(selected))
	}
	if !restorePlanComplete(selected, targetTXID) {
		t.Fatal("restore plan should cover target txid")
	}

	entries := restorePlanEntries(selected)
	if entries[0].ObjectPath != "0009/0000000000000001-00000000000210ff.ltx" {
		t.Fatalf("first object=%q", entries[0].ObjectPath)
	}
	if entries[len(entries)-1].MaxTXID != "0000000000021289" {
		t.Fatalf("last max txid=%q", entries[len(entries)-1].MaxTXID)
	}
}

func TestReplicaLevelSummariesIncludeEmptyLevels(t *testing.T) {
	candidates := parseRestorePlanCandidates(`
level min_txid max_txid size created
9 0000000000000001 00000000000210ff 197156405 2026-05-11T13:33:00Z
2 0000000000021100 0000000000021200 460226 2026-05-11T13:35:00Z
2 0000000000021201 0000000000021280 512000 2026-05-11T13:36:00Z
0 0000000000021281 0000000000021289 3796 2026-05-11T13:40:00Z
`)

	summaries := replicaLevelSummaries(candidates)
	if len(summaries) != 10 {
		t.Fatalf("replicaLevelSummaries() len = %d, want 10", len(summaries))
	}
	if summaries[0].Level != 0 || summaries[0].LevelName != "0000" {
		t.Fatalf("summaries[0] = %+v, want level 0000", summaries[0])
	}
	if summaries[2].ObjectCount != 2 {
		t.Fatalf("level 2 ObjectCount = %d, want 2", summaries[2].ObjectCount)
	}
	if summaries[2].TotalBytes != 972226 {
		t.Fatalf("level 2 TotalBytes = %d, want 972226", summaries[2].TotalBytes)
	}
	if summaries[2].MaxTXID != "0000000000021280" {
		t.Fatalf("level 2 MaxTXID = %q, want 0000000000021280", summaries[2].MaxTXID)
	}
	if summaries[9].ObjectCount != 1 {
		t.Fatalf("level 9 ObjectCount = %d, want 1", summaries[9].ObjectCount)
	}
	if summaries[1].ObjectCount != 0 || summaries[1].MaxTXID != "" {
		t.Fatalf("level 1 summary = %+v, want empty level", summaries[1])
	}
}
