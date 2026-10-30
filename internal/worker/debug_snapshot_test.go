package worker

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

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

func TestCollectReplicaLevelReportStreamsTheWholeListing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}
	dir := t.TempDir()
	script := `#!/bin/sh
[ "$1" = "ltx" ] || exit 2
echo "level min_txid max_txid size created"
i=0
while [ $i -lt 3000 ]; do
  printf '0 %016x %016x 207 2026-10-30T04:00:00Z\n' $i $i
  i=$((i+1))
done
printf '1 0000000000000001 00000000000042b0 5159392 2026-10-30T04:02:01Z\n'
printf '2 0000000000000001 0000000000003992 460226 2026-10-30T03:40:00Z\n'
printf '9 0000000000000001 00000000000040ff 197156405 2026-10-30T03:33:00Z\n'
`
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	cfg := DefaultConfig()
	cfg.ConfigPath = filepath.Join(dir, "litestream.yml")
	cfg.DBPath = filepath.Join(dir, "test.db")

	report := collectReplicaLevelReport(cfg, 0x4421)
	if report.Error != "" || !report.Complete || report.Truncated {
		t.Fatalf("report = error %q complete %v truncated %v", report.Error, report.Complete, report.Truncated)
	}
	if !usableReplicaLevelReport(report) {
		t.Fatal("complete report must be usable")
	}
	l0, l1, l2, l9 := replicaLevelSummary(report, 0), replicaLevelSummary(report, 1), replicaLevelSummary(report, 2), replicaLevelSummary(report, 9)
	if l0.ObjectCount != 3000 || l0.MaxTXIDDecimal != 2999 {
		t.Fatalf("L0 = %+v", l0)
	}
	if l1.ObjectCount != 1 || l1.MaxTXID != "00000000000042b0" || l2.MaxTXID != "0000000000003992" || l9.ObjectCount != 1 || l9.MaxTXID != "00000000000040ff" {
		t.Fatalf("high levels lost past the old 64 KB cap: L1=%+v L2=%+v L9=%+v", l1, l2, l9)
	}
	if len(report.OutputTail) != 0 {
		t.Fatalf("complete listing should not store an output tail: %d bytes", len(report.OutputTail))
	}
	if err := compactionProgressError(cfg, levelReport(map[int]reporting.ReplicaLevelSummary{0: {ObjectCount: 10, MaxTXIDDecimal: 0x3ab7}, 1: {ObjectCount: 1, MaxTXIDDecimal: 0x3eb4}}), report); err != nil {
		t.Fatalf("L1 advanced in the full listing but the check reported %v", err)
	}

	failing := `#!/bin/sh
echo "0 0000000000000001 0000000000000002 207 2026-10-30T04:00:00Z"
echo "replica unavailable" >&2
exit 1
`
	if err := os.WriteFile(filepath.Join(dir, "litestream"), []byte(failing), 0o700); err != nil {
		t.Fatal(err)
	}
	report = collectReplicaLevelReport(cfg, 0)
	if report.Error == "" || report.Complete || usableReplicaLevelReport(report) || !strings.Contains(report.OutputTail, "replica unavailable") {
		t.Fatalf("failed listing = %+v, want an unusable report with the stderr tail", report)
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
