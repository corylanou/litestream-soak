package model

import (
	"path/filepath"
	"testing"
)

func TestRecordRunArchiveIsIdempotent(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	archive := &RunArchive{
		DeploymentID:  42,
		Source:        "pr-1228",
		WorkerID:      "",
		ArchiveType:   "success",
		GitSHA:        "soak-sha",
		LitestreamSHA: "litestream-sha",
		ImageRef:      "registry.fly.io/litestream-soak:soak-sha",
		Status:        "stable",
		Summary:       "PR #1228 completed cleanly.",
		Payload:       `{"ok":true}`,
	}

	created, err := db.RecordRunArchive(archive)
	if err != nil {
		t.Fatalf("RecordRunArchive(first) error = %v", err)
	}
	if !created {
		t.Fatalf("RecordRunArchive(first) created = false, want true")
	}
	if archive.ID == 0 {
		t.Fatalf("archive.ID = 0, want assigned id")
	}

	second := *archive
	second.Summary = "different summary should not replace existing archive"
	created, err = db.RecordRunArchive(&second)
	if err != nil {
		t.Fatalf("RecordRunArchive(second) error = %v", err)
	}
	if created {
		t.Fatalf("RecordRunArchive(second) created = true, want false")
	}
	if second.ID != archive.ID {
		t.Fatalf("second.ID = %d, want %d", second.ID, archive.ID)
	}

	archives, err := db.ListRunArchives("pr-1228", "success", 10)
	if err != nil {
		t.Fatalf("ListRunArchives() error = %v", err)
	}
	if len(archives) != 1 {
		t.Fatalf("len(archives) = %d, want 1", len(archives))
	}
	if archives[0].Summary != "PR #1228 completed cleanly." {
		t.Fatalf("Summary = %q, want original summary", archives[0].Summary)
	}

	stored, err := db.GetRunArchive(archive.ID)
	if err != nil {
		t.Fatalf("GetRunArchive() error = %v", err)
	}
	if stored.Payload != `{"ok":true}` {
		t.Fatalf("Payload = %q", stored.Payload)
	}
}
