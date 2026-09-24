package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

const profileBlobSchema = `
 CREATE TABLE IF NOT EXISTS evidence_profile_blobs (id INTEGER PRIMARY KEY, digest TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS evidence_snapshot_compaction (name TEXT PRIMARY KEY, last_id INTEGER NOT NULL);`

const profileBlobCacheMaxBytes = 64 << 20

var profileSnapshotArrays = []struct{ field, refs string }{
	{"profile_incidents", "_profile_incident_blobs"},
	{"profile_records", "_profile_record_blobs"},
}

type profileBlobCache struct {
	mu        sync.Mutex
	ids       map[string]int64
	bodies    map[int64]json.RawMessage
	bodyBytes int
}

func newProfileBlobCache() *profileBlobCache {
	return &profileBlobCache{ids: make(map[string]int64), bodies: make(map[int64]json.RawMessage)}
}

func (c *profileBlobCache) id(digest string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.ids[digest]
	return id, ok
}

func (c *profileBlobCache) body(id int64) (json.RawMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	body, ok := c.bodies[id]
	return body, ok
}

func (c *profileBlobCache) store(digest string, id int64, body json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bodyBytes+len(body) > profileBlobCacheMaxBytes {
		clear(c.ids)
		clear(c.bodies)
		c.bodyBytes = 0
	}
	if digest != "" {
		c.ids[digest] = id
	}
	if _, ok := c.bodies[id]; !ok {
		c.bodies[id] = body
		c.bodyBytes += len(body)
	}
}

func hasProfileSnapshotArrays(raw string) bool {
	for _, spec := range profileSnapshotArrays {
		if strings.Contains(raw, `"`+spec.field+`"`) {
			return true
		}
	}
	return false
}

func hasProfileSnapshotRefs(raw string) bool {
	for _, spec := range profileSnapshotArrays {
		if strings.Contains(raw, `"`+spec.refs+`"`) {
			return true
		}
	}
	return false
}

func (d *DB) compactProfileSnapshot(raw string) (string, error) {
	if !hasProfileSnapshotArrays(raw) {
		return raw, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return raw, nil
	}
	changed := false
	for _, spec := range profileSnapshotArrays {
		encoded, ok := fields[spec.field]
		if !ok {
			continue
		}
		var items []json.RawMessage
		if err := json.Unmarshal(encoded, &items); err != nil || len(items) == 0 {
			continue
		}
		ids := make([]int64, len(items))
		for i, item := range items {
			id, err := d.storeProfileBlob(item)
			if err != nil {
				return "", err
			}
			ids[i] = id
		}
		refs, err := json.Marshal(ids)
		if err != nil {
			return "", err
		}
		delete(fields, spec.field)
		fields[spec.refs] = refs
		changed = true
	}
	if !changed {
		return raw, nil
	}
	compact, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return string(compact), nil
}

func (d *DB) storeProfileBlob(item json.RawMessage) (int64, error) {
	sum := sha256.Sum256(item)
	digest := hex.EncodeToString(sum[:])
	if id, ok := d.blobs.id(digest); ok {
		return id, nil
	}
	var id int64
	if err := d.writer.QueryRow(`INSERT INTO evidence_profile_blobs (digest, body) VALUES (?, ?)
		ON CONFLICT(digest) DO UPDATE SET digest=excluded.digest RETURNING id`, digest, string(item)).Scan(&id); err != nil {
		return 0, fmt.Errorf("store profile blob: %w", err)
	}
	d.blobs.store(digest, id, item)
	return id, nil
}

func (d *DB) expandProfileSnapshot(raw string) (string, error) {
	if !hasProfileSnapshotRefs(raw) {
		return raw, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return raw, nil
	}
	for _, spec := range profileSnapshotArrays {
		encoded, ok := fields[spec.refs]
		if !ok {
			continue
		}
		var ids []int64
		if err := json.Unmarshal(encoded, &ids); err != nil {
			return "", fmt.Errorf("decode %s: %w", spec.refs, err)
		}
		items, err := d.loadProfileBlobs(ids)
		if err != nil {
			return "", err
		}
		body, err := json.Marshal(items)
		if err != nil {
			return "", err
		}
		delete(fields, spec.refs)
		fields[spec.field] = body
	}
	expanded, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return string(expanded), nil
}

func (d *DB) loadProfileBlobs(ids []int64) ([]json.RawMessage, error) {
	items := make([]json.RawMessage, len(ids))
	var missing []int64
	seen := make(map[int64]bool)
	for i, id := range ids {
		if body, ok := d.blobs.body(id); ok {
			items[i] = body
			continue
		}
		if !seen[id] {
			seen[id] = true
			missing = append(missing, id)
		}
	}
	loaded := make(map[int64]json.RawMessage, len(missing))
	for start := 0; start < len(missing); start += 500 {
		chunk := missing[start:min(start+500, len(missing))]
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := d.query(`SELECT id, body FROM evidence_profile_blobs WHERE id IN (?`+strings.Repeat(",?", len(chunk)-1)+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var body string
			if err := rows.Scan(&id, &body); err != nil {
				_ = rows.Close()
				return nil, err
			}
			loaded[id] = json.RawMessage(body)
			d.blobs.store("", id, loaded[id])
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	for i, id := range ids {
		if items[i] != nil {
			continue
		}
		body, ok := loaded[id]
		if !ok {
			return nil, fmt.Errorf("profile blob %d is missing", id)
		}
		items[i] = body
	}
	return items, nil
}

type snapshotCompactionTarget struct {
	name, table, key, column string
}

var snapshotCompactionTargets = []snapshotCompactionTarget{
	{"evidence_verifications", "evidence_verifications", "id", "evidence_runtime"},
	{"evidence_events", "evidence_events", "journal_id", "details"},
}

func (d *DB) CompactProfileSnapshots(ctx context.Context, batchSize int) (int64, error) {
	if batchSize < 1 {
		return 0, fmt.Errorf("invalid compaction batch size %d", batchSize)
	}
	total, err := d.compactWorkerRuntimeSnapshots(ctx)
	if err != nil {
		return total, err
	}
	for _, target := range snapshotCompactionTargets {
		n, err := d.compactSnapshotTarget(ctx, target, batchSize)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (d *DB) compactWorkerRuntimeSnapshots(ctx context.Context) (int64, error) {
	rows, err := d.query(`SELECT id, last_runtime_json FROM workers WHERE instr(last_runtime_json, '"profile_incidents"') > 0 OR instr(last_runtime_json, '"profile_records"') > 0`)
	if err != nil {
		return 0, err
	}
	type entry struct{ id, raw string }
	var batch []entry
	for rows.Next() {
		var item entry
		if err := rows.Scan(&item.id, &item.raw); err != nil {
			_ = rows.Close()
			return 0, err
		}
		batch = append(batch, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, item := range batch {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		compact, err := d.compactProfileSnapshot(item.raw)
		if err != nil {
			return total, err
		}
		if compact == item.raw {
			continue
		}
		if _, err := d.writer.ExecContext(ctx, `UPDATE workers SET last_runtime_json=? WHERE id=? AND last_runtime_json=?`, compact, item.id, item.raw); err != nil {
			return total, err
		}
		total++
	}
	return total, nil
}

func (d *DB) compactSnapshotTarget(ctx context.Context, target snapshotCompactionTarget, batchSize int) (int64, error) {
	var cursor, maxID int64
	if err := d.queryRow(`SELECT COALESCE((SELECT last_id FROM evidence_snapshot_compaction WHERE name=?),0), (SELECT COALESCE(MAX(`+target.key+`),0) FROM `+target.table+`)`, target.name).Scan(&cursor, &maxID); err != nil {
		return 0, err
	}
	var total, removed int64
	for cursor < maxID {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		rows, err := d.query(`SELECT `+target.key+`, COALESCE(`+target.column+`,'') FROM `+target.table+` WHERE `+target.key+`>? AND `+target.key+`<=? ORDER BY `+target.key+` LIMIT ?`, cursor, maxID, batchSize)
		if err != nil {
			return total, err
		}
		type entry struct {
			key int64
			raw string
		}
		batch := make([]entry, 0, batchSize)
		for rows.Next() {
			var item entry
			if err := rows.Scan(&item.key, &item.raw); err != nil {
				_ = rows.Close()
				return total, err
			}
			batch = append(batch, item)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return total, err
		}
		if len(batch) == 0 {
			cursor = maxID
		} else {
			cursor = batch[len(batch)-1].key
		}
		type update struct {
			key     int64
			compact string
		}
		var updates []update
		for _, item := range batch {
			compact, err := d.compactProfileSnapshot(item.raw)
			if err != nil {
				return total, err
			}
			if compact != item.raw {
				removed += int64(len(item.raw) - len(compact))
				updates = append(updates, update{item.key, compact})
			}
		}
		tx, err := d.writer.BeginTx(ctx, nil)
		if err != nil {
			return total, err
		}
		for _, u := range updates {
			if _, err := tx.ExecContext(ctx, `UPDATE `+target.table+` SET `+target.column+`=? WHERE `+target.key+`=?`, u.compact, u.key); err != nil {
				_ = tx.Rollback()
				return total, err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO evidence_snapshot_compaction (name, last_id) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET last_id=excluded.last_id`, target.name, cursor); err != nil {
			_ = tx.Rollback()
			return total, err
		}
		if err := tx.Commit(); err != nil {
			return total, err
		}
		total += int64(len(batch))
		if total%1000 < int64(len(batch)) {
			slog.Info("Profile snapshot compaction progress", "table", target.table, "rows_processed", total, "bytes_removed", removed, "last_id", cursor)
		}
	}
	if total > 0 {
		slog.Info("Profile snapshot compaction complete", "table", target.table, "rows_processed", total, "bytes_removed", removed, "last_id", cursor)
	}
	return total, nil
}
