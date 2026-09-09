package worker

import (
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

const tenantLitestreamSHA = "4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3"

type TenantLifecycleOptions struct {
	SHA          string
	Capabilities string
	Binary       string
	Root         string
	Mode         string
	Tenants      int
	Timeout      time.Duration
	SyncTimeout  time.Duration
}

func (o TenantLifecycleOptions) validate() error {
	if decoded, err := hex.DecodeString(o.SHA); err != nil || len(decoded) != 20 {
		return fmt.Errorf("sha must be a full 40-character commit hash")
	}
	if o.Capabilities != "directory-v1" {
		return fmt.Errorf("unsupported capability contract: require explicit directory-v1")
	}
	if o.SyncTimeout <= 0 || o.SyncTimeout > o.Timeout {
		return fmt.Errorf("sync timeout must be positive and no greater than operation timeout")
	}
	if o.Mode != "watch" && o.Mode != "static" {
		return fmt.Errorf("mode must be watch or static")
	}
	if o.Tenants < 2 || o.Tenants > 1000 {
		return fmt.Errorf("tenants must be between 2 and 1000")
	}
	if o.Binary == "" || o.Root == "" || o.Timeout <= 0 || o.Timeout > 10*time.Minute {
		return fmt.Errorf("binary, root and timeout (0,10m] are required")
	}
	return nil
}

type tenantID struct{ Tenant, Generation int }

func (id tenantID) name() string {
	return fmt.Sprintf("tenant-%05d-g%02d.db", id.Tenant, id.Generation)
}

type tenantWork struct {
	since    time.Time
	verified time.Time
	pending  bool
	retired  bool
	attempt  uint64
}

type TenantLifecycleEvent struct {
	Phase  string    `json:"phase"`
	Tenant string    `json:"tenant,omitempty"`
	Status string    `json:"status"`
	Detail string    `json:"detail,omitempty"`
	At     time.Time `json:"at"`
}

type tenantLedger struct {
	limit       int
	generations map[int]int
	work        map[tenantID]*tenantWork
	sequence    uint64
	failures    []TenantLifecycleEvent
}

func newTenantLedger(limit int) *tenantLedger {
	return &tenantLedger{limit: limit, generations: make(map[int]int), work: make(map[tenantID]*tenantWork)}
}

func (q *tenantLedger) create(tenant int, now time.Time) (tenantID, error) {
	if tenant < 0 || tenant >= q.limit {
		return tenantID{}, fmt.Errorf("tenant limit exceeded")
	}
	previous := tenantID{tenant, q.generations[tenant]}
	if work := q.work[previous]; work != nil && !work.retired {
		return tenantID{}, fmt.Errorf("tenant is still live")
	}
	if previous.Generation >= 2 {
		return tenantID{}, fmt.Errorf("retained generation limit exceeded")
	}
	id := tenantID{tenant, previous.Generation + 1}
	q.generations[tenant] = id.Generation
	q.work[id] = &tenantWork{since: now, pending: true}
	return id, nil
}

func (q *tenantLedger) dirty(id tenantID, now time.Time) {
	w := q.work[id]
	if !w.pending {
		w.since = now
	}
	w.pending = true
}

func (q *tenantLedger) retire(id tenantID, now time.Time) error {
	w := q.work[id]
	if w == nil || w.pending || w.retired {
		return fmt.Errorf("retirement requires a verified live generation")
	}
	w.retired = true
	q.dirty(id, now)
	return nil
}

func (q *tenantLedger) pending() []tenantID {
	var ids []tenantID
	for id, w := range q.work {
		if w.pending {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := q.work[ids[i]], q.work[ids[j]]
		if a.attempt != b.attempt {
			return a.attempt < b.attempt
		}
		return ids[i].name() < ids[j].name()
	})
	return ids
}

func (q *tenantLedger) attempt(id tenantID, failure string, now time.Time) {
	q.sequence++
	q.work[id].attempt = q.sequence
	if failure != "" {
		q.failures = append(q.failures, TenantLifecycleEvent{Phase: "verify", Tenant: id.name(), Status: "failed", Detail: failure, At: now})
	}
}

func (q *tenantLedger) acknowledge(id tenantID, now time.Time) {
	if w := q.work[id]; w != nil {
		w.pending = false
		w.verified = now
	}
}

func (q *tenantLedger) oldestAge(now time.Time) float64 {
	oldest := now
	for _, w := range q.work {
		if w.pending && w.since.Before(oldest) {
			oldest = w.since
		}
	}
	return max(0, now.Sub(oldest).Seconds())
}
