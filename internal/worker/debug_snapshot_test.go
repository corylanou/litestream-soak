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
