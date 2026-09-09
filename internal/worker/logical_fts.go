package worker

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

var fts5Module = regexp.MustCompile("(?is)^CREATE\\s+VIRTUAL\\s+TABLE\\s+(?:\"(?:[^\"]|\"\")*\"|`(?:[^`]|``)*`|\\[[^\\]]+\\]|[^\\s(]+)\\s+USING\\s+fts5\\s*\\(")

func validateLogicalObject(ctx context.Context, tx *sql.Tx, name, kind string) error {
	if kind == "table" {
		return nil
	}
	if kind == "virtual" {
		var statement string
		if err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE name=? AND type='table'", name).Scan(&statement); err != nil {
			return err
		}
		if fts5Module.MatchString(statement) {
			return nil
		}
	}
	if kind == "shadow" {
		for _, suffix := range []string{"_data", "_idx", "_content", "_docsize", "_config"} {
			if !strings.HasSuffix(name, suffix) {
				continue
			}
			parent := strings.TrimSuffix(name, suffix)
			var statement string
			err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema JOIN pragma_table_list AS p ON p.name=sqlite_schema.name AND p.schema='main' WHERE sqlite_schema.name=? AND p.type='virtual'`, parent).Scan(&statement)
			if err == nil && fts5Module.MatchString(statement) {
				return nil
			}
			if err != nil && err != sql.ErrNoRows {
				return err
			}
		}
	}
	return fmt.Errorf("unsupported logical object type %q for %q", kind, name)
}
