package model

import (
	"errors"
	"strings"
	"time"
)

const runArchiveColumns = "id, deployment_id, source, worker_id, archive_type, git_sha, litestream_sha, image_ref, status, summary, payload, archived_at"

type runArchiveScanner interface {
	Scan(dest ...any) error
}

func scanRunArchive(scanner runArchiveScanner, archive *RunArchive) error {
	return scanner.Scan(
		&archive.ID,
		&archive.DeploymentID,
		&archive.Source,
		&archive.WorkerID,
		&archive.ArchiveType,
		&archive.GitSHA,
		&archive.LitestreamSHA,
		&archive.ImageRef,
		&archive.Status,
		&archive.Summary,
		&archive.Payload,
		&archive.ArchivedAt,
	)
}

func (d *DB) RecordRunArchive(archive *RunArchive) (bool, error) {
	if archive == nil {
		return false, errors.New("run archive is nil")
	}
	if strings.TrimSpace(archive.Payload) == "" {
		archive.Payload = "{}"
	}
	if archive.ArchivedAt.IsZero() {
		archive.ArchivedAt = time.Now().UTC()
	}

	result, err := d.db.Exec(`
		INSERT OR IGNORE INTO run_archives (deployment_id, source, worker_id, archive_type, git_sha, litestream_sha, image_ref, status, summary, payload, archived_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		archive.DeploymentID,
		archive.Source,
		archive.WorkerID,
		archive.ArchiveType,
		archive.GitSHA,
		archive.LitestreamSHA,
		archive.ImageRef,
		archive.Status,
		archive.Summary,
		archive.Payload,
		archive.ArchivedAt,
	)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rowsAffected == 0 {
		err := d.db.QueryRow(`
			SELECT id, archived_at
			FROM run_archives
			WHERE deployment_id = ? AND archive_type = ? AND worker_id = ?`,
			archive.DeploymentID,
			archive.ArchiveType,
			archive.WorkerID,
		).Scan(&archive.ID, &archive.ArchivedAt)
		return false, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return false, err
	}
	archive.ID = int(id)
	return true, nil
}

func (d *DB) ListRunArchives(source, archiveType string, limit int) ([]RunArchive, error) {
	if limit <= 0 {
		limit = 20
	}

	query := "SELECT " + runArchiveColumns + " FROM run_archives"
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if strings.TrimSpace(source) != "" {
		clauses = append(clauses, "source = ?")
		args = append(args, strings.TrimSpace(source))
	}
	if strings.TrimSpace(archiveType) != "" {
		clauses = append(clauses, "archive_type = ?")
		args = append(args, strings.TrimSpace(archiveType))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY archived_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	archives := make([]RunArchive, 0)
	for rows.Next() {
		var archive RunArchive
		if err := scanRunArchive(rows, &archive); err != nil {
			return nil, err
		}
		archives = append(archives, archive)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return archives, nil
}

func (d *DB) GetRunArchive(id int) (*RunArchive, error) {
	var archive RunArchive
	err := scanRunArchive(
		d.db.QueryRow("SELECT "+runArchiveColumns+" FROM run_archives WHERE id = ?", id),
		&archive,
	)
	if err != nil {
		return nil, err
	}
	return &archive, nil
}
