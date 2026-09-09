package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"net/url"
	"strings"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const logicalValidatorVersion = "soak-logical-v1"

func (c Config) logicalLimits() logicalLimits {
	return logicalLimits{rows: c.LogicalMaxRows, bytes: c.LogicalMaxBytes, objects: c.LogicalMaxObjects, valueBytes: c.LogicalMaxValueBytes}
}

type logicalLimits struct {
	rows       int64
	bytes      int64
	objects    int
	valueBytes int
}

type logicalTable struct {
	name   string
	rows   int64
	digest [32]byte
}

type logicalSnapshot struct {
	schema [32]byte
	tables []logicalTable
}

func readLogicalSnapshot(ctx context.Context, path string, limits logicalLimits) (logicalSnapshot, error) {
	return readLogicalSnapshotMode(ctx, path, limits, false)
}

func readLogicalSnapshotMode(ctx context.Context, path string, limits logicalLimits, schemaOnly bool) (logicalSnapshot, error) {
	var snapshot logicalSnapshot
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return snapshot, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return snapshot, err
	}
	defer func() { _ = conn.Close() }()
	if _, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_LENGTH, limits.valueBytes); err != nil {
		return snapshot, err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; PRAGMA temp.cache_size=-2048; PRAGMA threads=0; PRAGMA query_only=ON; PRAGMA trusted_schema=OFF"); err != nil {
		return snapshot, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return snapshot, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, "SELECT type, name, tbl_name, coalesce(sql, '') FROM sqlite_schema ORDER BY type, name")
	if err != nil {
		return snapshot, err
	}

	schemaHash := sha256.New()
	for _, pragma := range []string{"user_version", "application_id"} {
		var value int64
		if err := tx.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
			_ = rows.Close()
			return snapshot, err
		}
		_, _ = fmt.Fprintf(schemaHash, "%s:%d;", pragma, value)
	}

	objects := 0
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			_ = rows.Close()
			return snapshot, err
		}
		objects++
		if len(name) > 1024 {
			_ = rows.Close()
			return snapshot, fmt.Errorf("logical object name limit exceeded: 1024 bytes")
		}
		limits.bytes -= int64(len(kind) + len(name) + len(table) + len(statement))
		if limits.bytes < 0 {
			_ = rows.Close()
			return snapshot, fmt.Errorf("logical byte limit exceeded in schema")
		}
		if objects > limits.objects {
			_ = rows.Close()
			return snapshot, fmt.Errorf("logical object limit exceeded: %d", limits.objects)
		}

		for _, value := range []string{kind, name, table, statement} {
			_, _ = fmt.Fprintf(schemaHash, "%d:%s", len(value), value)
		}
		if kind == "table" {
			snapshot.tables = append(snapshot.tables, logicalTable{name: name})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return snapshot, err
	}
	if err := rows.Close(); err != nil {
		return snapshot, err
	}
	copy(snapshot.schema[:], schemaHash.Sum(nil))
	if schemaOnly {
		snapshot.tables = nil
		return snapshot, nil
	}
	for i := range snapshot.tables {
		table, err := readLogicalTable(ctx, tx, snapshot.tables[i].name, &limits)
		if err != nil {
			return snapshot, fmt.Errorf("table %q: %w", snapshot.tables[i].name, err)
		}
		snapshot.tables[i] = table
	}
	ftsObjects := 0
	for _, table := range snapshot.tables {
		switch table.name {
		case "fts_search", "fts_documents", "fts_progress":
			ftsObjects++
		}
	}
	if ftsObjects == 3 {
		if _, err := checkFTSSearch(ctx, tx); err != nil {
			return snapshot, err
		}
	}
	return snapshot, nil
}

func logicalIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func readLogicalTable(ctx context.Context, tx *sql.Tx, name string, limits *logicalLimits) (logicalTable, error) {
	table := logicalTable{name: name}
	if name == "_litestream_seq" {
		var statement string
		if err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", name).Scan(&statement); err != nil {
			return table, err
		}
		if strings.ToLower(strings.Join(strings.Fields(statement), " ")) == "create table _litestream_seq (id integer primary key, seq integer)" {
			var invalid int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM _litestream_seq WHERE id != 1 OR typeof(seq) != 'integer' OR seq < 1").Scan(&invalid); err != nil {
				return table, err
			}
			if invalid != 0 {
				return table, fmt.Errorf("unsupported _litestream_seq bookkeeping contents")
			}
			return table, nil
		}
	}

	columns, err := tx.QueryContext(ctx, "SELECT name, pk FROM pragma_table_xinfo(?) WHERE hidden != 1 ORDER BY cid", name)
	if err != nil {
		return table, err
	}
	var expressions, ordering []string
	primaryKey := make(map[int]string)
	columnNames := make(map[string]bool)
	for columns.Next() {
		var column string
		var pk int
		if err := columns.Scan(&column, &pk); err != nil {
			_ = columns.Close()
			return table, err
		}
		if len(column) > 1024 {
			_ = columns.Close()
			return table, fmt.Errorf("logical column name limit exceeded: 1024 bytes")
		}
		columnNames[strings.ToLower(column)] = true
		if pk > 0 {
			primaryKey[pk] = logicalIdentifier(column)
		}
		expression := "+" + logicalIdentifier(column)
		expressions = append(expressions, expression)
		ordering = append(ordering, "typeof("+expression+")", expression+" COLLATE BINARY")
		if len(expressions) > 256 {
			_ = columns.Close()
			return table, fmt.Errorf("logical column limit exceeded: 256")
		}
	}
	if err := columns.Err(); err != nil {
		_ = columns.Close()
		return table, err
	}
	if err := columns.Close(); err != nil {
		return table, err
	}
	var kind string
	var withoutRowID int
	if err := tx.QueryRowContext(ctx, "SELECT type, wr FROM pragma_table_list WHERE schema='main' AND name=?", name).Scan(&kind, &withoutRowID); err != nil {
		return table, err
	}
	if err := validateLogicalObject(ctx, tx, name, kind); err != nil {
		return table, err
	}
	if withoutRowID != 0 {
		ordering = nil
		for position := 1; position <= len(primaryKey); position++ {
			ordering = append(ordering, primaryKey[position])
		}
	}
	if withoutRowID == 0 {
		for _, alias := range []string{"rowid", "_rowid_", "oid"} {
			if !columnNames[alias] {
				expressions = append(expressions, alias)
				ordering = []string{alias}
				break
			}
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(expressions, ",")+" FROM "+logicalIdentifier(name)+" ORDER BY "+strings.Join(ordering, ","))
	if err != nil {
		return table, err
	}
	defer func() { _ = rows.Close() }()
	values := make([]any, len(expressions))
	targets := make([]any, len(values))
	for i := range values {
		targets[i] = &values[i]
	}
	digest := sha256.New()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return table, err
		}
		if limits.rows <= 0 {
			return table, fmt.Errorf("logical row limit exceeded")
		}
		limits.rows--
		if err := rows.Scan(targets...); err != nil {
			return table, err
		}
		encoded, err := encodeLogicalRow(values)
		if err != nil {
			return table, err
		}
		limits.bytes -= int64(len(encoded))
		if limits.bytes < 0 {
			return table, fmt.Errorf("logical byte limit exceeded")
		}
		hash := sha256.Sum256(encoded)
		_, _ = digest.Write(hash[:])
		table.rows++
	}
	if err := rows.Err(); err != nil {
		return table, err
	}
	copy(table.digest[:], digest.Sum(nil))
	return table, nil
}

func encodeLogicalRow(values []any) ([]byte, error) {
	var encoded []byte
	for _, value := range values {
		switch value := value.(type) {
		case nil:
			encoded = append(encoded, 'n')
		case int64:
			encoded = append(encoded, 'i')
			encoded = binary.BigEndian.AppendUint64(encoded, uint64(value))
		case float64:
			if value == 0 {
				value = 0
			}
			encoded = append(encoded, 'r')
			encoded = binary.BigEndian.AppendUint64(encoded, math.Float64bits(value))
		case string:
			encoded = append(encoded, 't')
			encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(value)))
			encoded = append(encoded, value...)
		case []byte:
			encoded = append(encoded, 'b')
			encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(value)))
			encoded = append(encoded, value...)
		default:
			return nil, fmt.Errorf("unsupported logical value type %T", value)
		}
	}
	return encoded, nil
}

func compareLogicalSnapshots(source, restored logicalSnapshot) error {
	if source.schema != restored.schema {
		return fmt.Errorf("logical schema mismatch: source_sha256=%x restored_sha256=%x", source.schema, restored.schema)
	}
	for i, expected := range source.tables {
		if i >= len(restored.tables) {
			return fmt.Errorf("logical table %q missing", expected.name)
		}
		actual := restored.tables[i]
		if expected != actual {
			return fmt.Errorf("logical table %q mismatch: source_rows=%d restored_rows=%d source_sha256=%x restored_sha256=%x", expected.name, expected.rows, actual.rows, expected.digest, actual.digest)
		}
	}
	return nil
}

func (v *Verifier) logicalValidation(ctx context.Context, sourcePath string, txid uint64) (logicalSnapshot, error) {
	v.logicalEvidence = fmt.Sprintf("validator=%s workload=soak:%s synthetic_workload=litestream:%s boundary_txid=%016x database=%q bookkeeping=recognized-sequence-content-excluded", logicalValidatorVersion, v.cfg.GitSHA, v.logicalWorkloadSHA, txid, sourcePath)
	snapshot, err := readLogicalSnapshot(ctx, sourcePath, v.cfg.logicalLimits())
	if err != nil {
		return snapshot, fmt.Errorf("%s: source snapshot: %w", v.logicalEvidence, err)
	}
	return snapshot, nil
}

func (v *Verifier) compareRestoredLogical(ctx context.Context, source logicalSnapshot, restoredPath string) error {
	restored, err := readLogicalSnapshot(ctx, restoredPath, v.cfg.logicalLimits())
	if err == nil {
		err = compareLogicalSnapshots(source, restored)
	}
	if err != nil {
		return fmt.Errorf("%s: restored comparison: %w", v.logicalEvidence, err)
	}
	if v.cfg.churnEnabled() {
		if err := validateChurnRestore(ctx, v.cfg, restoredPath); err != nil {
			return err
		}
		v.logicalEvidence += " application_invariants=true"
	}
	v.logicalEvidence += fmt.Sprintf(" schema_sha256=%x tables=%d logical_match=true", source.schema, len(source.tables))
	return nil
}
