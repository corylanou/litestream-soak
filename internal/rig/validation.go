package rig

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
)

func ValidatePrefix(ctx context.Context, source, restored *sql.DB, minimumRows, transactionRows int) error {
	rows, err := restored.QueryContext(ctx, "SELECT id, value, typeof(value) FROM t ORDER BY id")
	if err != nil {
		return fmt.Errorf("read restored rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	expected, err := source.QueryContext(ctx, "SELECT id, value, typeof(value) FROM t ORDER BY id")
	if err != nil {
		return fmt.Errorf("read source rows: %w", err)
	}
	defer func() { _ = expected.Close() }()
	count := 0
	for rows.Next() {
		var id, expectedID int64
		var value, expectedValue []byte
		var valueType, expectedType string
		if err := rows.Scan(&id, &value, &valueType); err != nil {
			return err
		}
		if !expected.Next() {
			if err := expected.Err(); err != nil {
				return err
			}
			return fmt.Errorf("unexpected restored row %d", id)
		}
		if err := expected.Scan(&expectedID, &expectedValue, &expectedType); err != nil {
			return err
		}
		if id != expectedID || valueType != expectedType || !bytes.Equal(value, expectedValue) {
			return fmt.Errorf("restored row %d differs from source row %d", id, expectedID)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count == 0 || count < minimumRows {
		return fmt.Errorf("restored %d rows, require at least %d committed rows", count, minimumRows)
	}
	if transactionRows <= 0 || count%transactionRows != 0 {
		return fmt.Errorf("restored %d rows outside transaction boundary %d", count, transactionRows)
	}
	return nil
}
