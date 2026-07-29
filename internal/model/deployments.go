package model

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const deploymentColumns = "id, git_sha, litestream_sha, image_ref, source, repository, pr_number, status, started_at, completed_at, error_message"

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
		&dep.Repository,
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
		INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, repository, pr_number, status)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, strings.TrimSpace(dep.Repository), dep.PRNumber, dep.Status,
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
	tx, err := d.writer.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	repository := strings.TrimSpace(dep.Repository)
	boundRepository, err := deploymentSourceRepository(tx, dep.Source)
	if err != nil {
		return err
	}
	var sourceDeploymentCount int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM deployments WHERE source = ?`, dep.Source).Scan(&sourceDeploymentCount); err != nil {
		return err
	}
	if boundRepository != "" {
		if repository != "" && !strings.EqualFold(repository, boundRepository) {
			return fmt.Errorf("deployment source %s is bound to repository %s, not %s", dep.Source, boundRepository, repository)
		}
		repository = boundRepository
	} else if repository != "" && sourceDeploymentCount > 0 {
		return fmt.Errorf("deployment source %s has existing deployments without a repository binding", dep.Source)
	}

	var existingID int64
	err = tx.QueryRow(
		`SELECT id FROM deployments WHERE source = ? AND git_sha = ? AND litestream_sha = ? ORDER BY started_at DESC, id DESC LIMIT 1`,
		dep.Source,
		dep.GitSHA,
		dep.LitestreamSHA,
	).Scan(&existingID)
	switch {
	case err == nil:
		_, err = tx.Exec(`
			UPDATE deployments
			SET status = 'ready', image_ref = ?, repository = ?, error_message = '', completed_at = datetime('now')
			WHERE id = ?`,
			dep.ImageRef,
			repository,
			existingID,
		)
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.Exec(`
			INSERT INTO deployments (git_sha, litestream_sha, image_ref, source, repository, pr_number, status, started_at, completed_at, error_message)
			VALUES (?, ?, ?, ?, ?, ?, 'ready', datetime('now'), datetime('now'), '')`,
			dep.GitSHA, dep.LitestreamSHA, dep.ImageRef, dep.Source, repository, dep.PRNumber,
		)
	default:
		return err
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) GetDeploymentSourceRepository(source string) (string, error) {
	return deploymentSourceRepository(d.reader, source)
}

type deploymentRepositoryQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func deploymentSourceRepository(querier deploymentRepositoryQuerier, source string) (string, error) {
	rows, err := querier.Query(
		`SELECT DISTINCT repository FROM deployments WHERE source = ? AND TRIM(repository) <> ''`,
		source,
	)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	repository := ""
	for rows.Next() {
		var candidate string
		if err := rows.Scan(&candidate); err != nil {
			return "", err
		}
		candidate = strings.TrimSpace(candidate)
		if repository != "" && !strings.EqualFold(repository, candidate) {
			return "", fmt.Errorf("deployment source %s has conflicting repositories %s and %s", source, repository, candidate)
		}
		repository = candidate
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return repository, nil
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
