package churn

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Config struct {
	Slots       int    `json:"slots"`
	HotPercent  int    `json:"hot_percent"`
	Workers     int    `json:"workers"`
	Rate        int    `json:"rate"`
	PayloadSize int    `json:"payload_size"`
	Seed        uint64 `json:"seed"`
}

func (c Config) Validate() error {
	if c.Slots < 1 || c.Slots > 1000000 || c.HotPercent < 0 || c.HotPercent > 100 || c.Workers < 1 || c.Workers > 64 || c.Rate < 1 || c.Rate > 100000 || c.PayloadSize < 1 || c.PayloadSize > 65536 {
		return fmt.Errorf("churn requires slots 1..1000000, hot_percent 0..100, workers 1..64, rate 1..100000, payload_size 1..65536")
	}
	return nil
}

func (c Config) JSON() string {
	b, _ := json.Marshal(c)
	return string(b)
}

type Observer func(string, int64, time.Duration, error)

type Engine struct {
	mode    string
	cfg     Config
	observe Observer
	mu      sync.Mutex
	paused  bool
	active  int
	next    uint64
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type operation struct {
	name string
	key  int
	tick int64
}

func New(mode string, cfg Config, observe Observer) *Engine {
	return &Engine{mode: mode, cfg: cfg, observe: observe}
}

func (e *Engine) operation(n uint64) operation {
	ops := []string{"enqueue", "claim", "retry", "claim", "complete", "delete", "enqueue", "expire", "delete"}
	if e.mode == "cache" {
		ops = []string{"upsert", "upsert", "evict"}
	}
	group := n / uint64(len(ops))
	h := (group+e.cfg.Seed)*6364136223846793005 + 1442695040888963407
	slots := e.cfg.Slots
	if int(h%100) < e.cfg.HotPercent {
		slots = max(1, slots/10)
	}
	return operation{name: ops[n%uint64(len(ops))], key: int((h >> 16) % uint64(slots)), tick: int64(n)}
}

func (e *Engine) Start(ctx context.Context, db *sql.DB) error {
	if err := e.cfg.Validate(); err != nil {
		return err
	}
	if e.mode != "queue" && e.mode != "cache" {
		return fmt.Errorf("unknown churn mode %q", e.mode)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cancel != nil {
		return fmt.Errorf("churn engine already started")
	}
	ctx, e.cancel = context.WithCancel(ctx)
	jobs := make(chan operation)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer close(jobs)
		ticker := time.NewTicker(time.Second / time.Duration(e.cfg.Rate))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.mu.Lock()
				paused := e.paused
				e.mu.Unlock()
				if paused {
					continue
				}
				select {
				case jobs <- e.operation(e.next):
					e.next++
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for op := range jobs {
				e.mu.Lock()
				if e.paused || ctx.Err() != nil {
					e.mu.Unlock()
					continue
				}
				e.active++
				e.mu.Unlock()
				started := time.Now()
				n, err := Apply(ctx, db, e.mode, op.name, op.key, op.tick, e.cfg.PayloadSize)
				if e.observe != nil {
					e.observe(op.name, n, time.Since(started), err)
				}
				e.mu.Lock()
				e.active--
				e.mu.Unlock()
			}
		}()
	}
	return nil
}

func (e *Engine) Pause(ctx context.Context) error {
	e.mu.Lock()
	e.paused = true
	e.mu.Unlock()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		e.mu.Lock()
		active := e.active
		e.mu.Unlock()
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Engine) Resume() {
	e.mu.Lock()
	e.paused = false
	e.mu.Unlock()
}
func (e *Engine) Stop() {
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.wg.Wait()
}
