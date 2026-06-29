package model

import (
	"database/sql"
	"errors"
)

const deploymentColumns = "id, git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message"

type deploymentScanner interface {
	Scan(dest ...any) error
}

func scanDeployment(scanner deploymentScanner, dep *Deployment) error {
	var completedAt sql.NullTime
	var prNumber sql.NullInt64
	var errorMessage sql.NullString
	if err := scanner.Scan(
		&dep.ID,
		&dep.GitSHA,
		&dep.LitestreamSHA,
		&dep.ImageRef,
		&dep.Source,
		&prNumber,
		&dep.Status,
		&dep.StartedAt,
		&completedAt,
		&errorMessage,
	); err != nil {
		return err
	}

	if prNumber.Valid {
		dep.PRNumber = int(prNumber.Int64)
	}
	if completedAt.Valid {
		dep.CompletedAt = &completedAt.Time
	}
	if errorMessage.Valid {
		dep.ErrorMessage = errorMessage.String
	}
	return nil
}

func (d *DB) CreateDeployment(dep *Deployment) (int64, error) {
	result, err := d.exec(`
		INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, pr_number, status)
		VALUES (?, ?, ?, ?, ?, ?)`,
		dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, dep.PRNumber, dep.Status,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (d *DB) ListDeployments(source string, limit int) ([]Deployment, error) {
	query := "SELECT " + deploymentColumns + " FROM deployments"
	args := make([]any, 0, 2)
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY started_at DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := d.query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	deployments := make([]Deployment, 0)
	for rows.Next() {
		var dep Deployment
		if err := scanDeployment(rows, &dep); err != nil {
			return nil, err
		}
		deployments = append(deployments, dep)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deployments, nil
}

func (d *DB) UpdateDeployment(id int64, status, imageRef, errMsg string) error {
	_, err := d.exec(`
		UPDATE deployments SET status = ?, image_ref = ?, error_message = ?, completed_at = datetime('now')
		WHERE id = ?`,
		status, imageRef, errMsg, id,
	)
	return err
}

func (d *DB) UpsertReadyDeployment(dep *Deployment) error {
	existing, err := d.GetDeploymentByVersion(dep.Source, dep.GitSHA, dep.LitestreamSHA)
	switch {
	case err == nil:
		return d.UpdateDeployment(int64(existing.ID), "ready", dep.ImageRef, "")
	case errors.Is(err, sql.ErrNoRows):
		_, err := d.exec(`
			INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, pr_number, status, started_at, completed_at, error_message)
			VALUES (?, ?, ?, ?, ?, 'ready', datetime('now'), datetime('now'), '')`,
			dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, dep.PRNumber,
		)
		return err
	default:
		return err
	}
}

func (d *DB) GetDeploymentBySHA(sha string) (*Deployment, error) {
	var dep Deployment
	err := scanDeployment(
		d.queryRow("SELECT "+deploymentColumns+" FROM deployments WHERE git_sha = ? ORDER BY started_at DESC LIMIT 1", sha),
		&dep,
	)
	if err != nil {
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetDeploymentByVersion(source, gitSHA, litestreamSHA string) (*Deployment, error) {
	var dep Deployment
	err := scanDeployment(
		d.queryRow("SELECT "+deploymentColumns+" FROM deployments WHERE source = ? AND git_sha = ? AND litestream_sha = ? ORDER BY started_at DESC, id DESC LIMIT 1",
			source,
			gitSHA,
			litestreamSHA,
		),
		&dep,
	)
	if err != nil {
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetLatestDeployment(source string) (*Deployment, error) {
	query := "SELECT " + deploymentColumns + " FROM deployments"
	args := make([]any, 0, 1)
	if source != "" {
		query += " WHERE source = ?"
		args = append(args, source)
	}
	query += " ORDER BY started_at DESC, id DESC LIMIT 1"

	var dep Deployment
	err := scanDeployment(d.queryRow(query, args...), &dep)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &dep, nil
}

func (d *DB) GetLatestReadyDeployment(source string) (*Deployment, error) {
	query := "SELECT " + deploymentColumns + " FROM deployments WHERE status = 'ready'"
	args := make([]any, 0, 1)
	if source != "" {
		query += " AND source = ?"
		args = append(args, source)
	}
	query += " ORDER BY started_at DESC, id DESC LIMIT 1"

	var dep Deployment
	err := scanDeployment(d.queryRow(query, args...), &dep)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &dep, nil
}
