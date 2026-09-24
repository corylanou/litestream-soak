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
	"time"
)

const profileBlobSchema = `
 CREATE TABLE IF NOT EXISTS evidence_profile_blobs (id INTEGER PRIMARY KEY, digest TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS evidence_snapshot_compaction (name TEXT PRIMARY KEY, last_id INTEGER NOT NULL);`

const profileBlobCacheMaxBytes = 64 << 20

const profileBlobSweepInterval = 24 * time.Hour

var profileBlobReferenceColumns = []struct{ table, column string }{
	{"workers", "last_runtime_json"},
	{"evidence_verifications", "evidence_runtime"},
	{"evidence_events", "details"},
	{"events", "details"},
}

var profileSnapshotArrays = []struct{ field, refs string }{
	{"profile_incidents", "_profile_incident_blobs"},
	{"profile_records", "_profile_record_blobs"},
}

type profileBlobCache struct {
	sweep     sync.RWMutex
	mu        sync.Mutex
	ids       map[string]int64
	bodies    map[int64]json.RawMessage
	bodyBytes int
	pinning   bool
	pinned    map[int64]bool
}

func (c *profileBlobCache) hold() func() {
	c.sweep.RLock()
	return c.sweep.RUnlock
}

func (c *profileBlobCache) pin(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pinning {
		c.pinned[id] = true
	}
}

func (c *profileBlobCache) startPinning() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinning = true
	c.pinned = make(map[int64]bool)
}

func (c *profileBlobCache) isPinned(id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pinned[id]
}

func (c *profileBlobCache) clearCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.ids)
	clear(c.bodies)
	c.bodyBytes = 0
}

func (c *profileBlobCache) finishSweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pinning = false
	c.pinned = nil
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
		d.blobs.pin(id)
		return id, nil
	}
	var id int64
	if err := d.writer.QueryRow(`INSERT INTO evidence_profile_blobs (digest, body) VALUES (?, ?)
		ON CONFLICT(digest) DO UPDATE SET digest=excluded.digest RETURNING id`, digest, string(item)).Scan(&id); err != nil {
		return 0, fmt.Errorf("store profile blob: %w", err)
	}
	d.blobs.store(digest, id, item)
	d.blobs.pin(id)
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
		release := d.blobs.hold()
		compact, err := d.compactProfileSnapshot(item.raw)
		if err == nil && compact != item.raw {
			_, err = d.writer.ExecContext(ctx, `UPDATE workers SET last_runtime_json=? WHERE id=? AND last_runtime_json=?`, compact, item.id, item.raw)
			total++
		}
		release()
		if err != nil {
			return total, err
		}
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
		var updates []snapshotUpdate
		release := d.blobs.hold()
		for _, item := range batch {
			compact, err := d.compactProfileSnapshot(item.raw)
			if err != nil {
				release()
				return total, err
			}
			if compact != item.raw {
				removed += int64(len(item.raw) - len(compact))
				updates = append(updates, snapshotUpdate{item.key, compact})
			}
		}
		err = d.applySnapshotUpdates(ctx, target, updates, cursor)
		release()
		if err != nil {
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

type snapshotUpdate struct {
	key     int64
	compact string
}

func (d *DB) applySnapshotUpdates(ctx context.Context, target snapshotCompactionTarget, updates []snapshotUpdate, cursor int64) error {
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, u := range updates {
		if _, err := tx.ExecContext(ctx, `UPDATE `+target.table+` SET `+target.column+`=? WHERE `+target.key+`=?`, u.compact, u.key); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO evidence_snapshot_compaction (name, last_id) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET last_id=excluded.last_id`, target.name, cursor); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (d *DB) PruneProfileBlobs(ctx context.Context) (int64, error) {
	var last int64
	if err := d.queryRow(`SELECT COALESCE((SELECT last_id FROM evidence_snapshot_compaction WHERE name='profile_blob_sweep'),0)`).Scan(&last); err != nil {
		return 0, err
	}
	if time.Since(time.Unix(last, 0)) < profileBlobSweepInterval {
		return 0, nil
	}

	d.blobs.sweep.Lock()
	d.blobs.startPinning()
	d.blobs.sweep.Unlock()
	defer func() {
		d.blobs.sweep.Lock()
		d.blobs.finishSweep()
		d.blobs.sweep.Unlock()
	}()

	var maxID int64
	if err := d.queryRow(`SELECT COALESCE(MAX(id),0) FROM evidence_profile_blobs`).Scan(&maxID); err != nil {
		return 0, err
	}
	referenced, err := d.referencedProfileBlobs(ctx)
	if err != nil {
		return 0, err
	}
	candidates, err := d.unreferencedProfileBlobs(ctx, maxID, referenced)
	if err != nil {
		return 0, err
	}

	var deleted int64
	cleared := false
	for start := 0; start < len(candidates); start += profileBlobDeleteBatch {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		n, err := d.deleteProfileBlobBatch(ctx, candidates[start:min(start+profileBlobDeleteBatch, len(candidates))], !cleared)
		deleted += n
		cleared = true
		if err != nil {
			return deleted, err
		}
	}
	if _, err := d.writer.ExecContext(ctx, `INSERT INTO evidence_snapshot_compaction (name, last_id) VALUES ('profile_blob_sweep', ?) ON CONFLICT(name) DO UPDATE SET last_id=excluded.last_id`, time.Now().Unix()); err != nil {
		return deleted, err
	}
	if deleted > 0 {
		slog.Info("Pruned unreferenced profile blobs", "deleted", deleted, "referenced", len(referenced))
	}
	return deleted, nil
}

const profileBlobDeleteBatch = 500

func (d *DB) unreferencedProfileBlobs(ctx context.Context, maxID int64, referenced map[int64]bool) ([]int64, error) {
	rows, err := d.reader.QueryContext(ctx, `SELECT id FROM evidence_profile_blobs WHERE id<=?`, maxID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var candidates []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if !referenced[id] {
			candidates = append(candidates, id)
		}
	}
	return candidates, rows.Err()
}

func (d *DB) deleteProfileBlobBatch(ctx context.Context, ids []int64, clearCache bool) (int64, error) {
	d.blobs.sweep.Lock()
	defer d.blobs.sweep.Unlock()
	if clearCache {
		d.blobs.clearCache()
	}
	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, id := range ids {
		if d.blobs.isPinned(id) {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM evidence_profile_blobs WHERE id=?`, id); err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		deleted++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (d *DB) referencedProfileBlobs(ctx context.Context) (map[int64]bool, error) {
	referenced := make(map[int64]bool)
	for _, source := range profileBlobReferenceColumns {
		rows, err := d.reader.QueryContext(ctx, `SELECT `+source.column+` FROM `+source.table+` WHERE instr(`+source.column+`, '"_profile_') > 0`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				continue
			}
			for _, spec := range profileSnapshotArrays {
				var ids []int64
				if err := json.Unmarshal(fields[spec.refs], &ids); err != nil {
					continue
				}
				for _, id := range ids {
					referenced[id] = true
				}
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return referenced, nil
}
