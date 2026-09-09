package replay

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

type scheduledAdapter struct {
	timestamps []time.Time
	insert     func(int) error
}

func (a scheduledAdapter) Name() string               { return "scheduled" }
func (a scheduledAdapter) CreateTables(*sql.DB) error { return nil }
func (a scheduledAdapter) Rows() (RowIterator, error) {
	return &scheduledIterator{scheduledAdapter: a}, nil
}

type scheduledIterator struct {
	scheduledAdapter
	index int
}

func (it *scheduledIterator) Next() bool           { it.index++; return it.index <= len(it.timestamps) }
func (it *scheduledIterator) Timestamp() time.Time { return it.timestamps[it.index-1] }
func (it *scheduledIterator) Insert(*sql.DB) error { return it.insert(it.index - 1) }
func (it *scheduledIterator) Err() error           { return nil }
func (it *scheduledIterator) Close() error         { return nil }

func TestEngineSchedule(t *testing.T) {
	for _, tc := range []struct {
		name         string
		gap, latency time.Duration
		speed        float64
		want         time.Duration
	}{
		{"long gap", 20 * time.Second, 0, 2, 10 * time.Second},
		{"insertion consumes gap", 4 * time.Second, time.Second, 2, 2 * time.Second},
		{"behind schedule", time.Second, 3 * time.Second, 1, 3 * time.Second},
		{"default speed", time.Second, 0, 0, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				var second time.Duration
				engine := NewEngine(Config{SpeedMultiplier: tc.speed, WorkerID: t.Name()}, scheduledAdapter{
					timestamps: []time.Time{time.Unix(0, 0), time.Unix(0, 0).Add(tc.gap)},
					insert: func(i int) error {
						if i == 0 {
							time.Sleep(tc.latency)
						} else {
							second = time.Since(start)
						}
						return nil
					},
				})
				if err := engine.replayOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
				if second != tc.want {
					t.Fatalf("second insert at %v, want %v", second, tc.want)
				}
				scaledGap := tc.gap
				if tc.speed > 0 {
					scaledGap = time.Duration(float64(tc.gap) / tc.speed)
				}
				wantLag := max(time.Duration(0), tc.latency-scaledGap)
				if got := testutil.ToFloat64(replayLagSeconds.WithLabelValues(engine.metricLabels("scheduled")...)); got != wantLag.Seconds() {
					t.Fatalf("lag=%v, want %v", got, wantLag.Seconds())
				}
			})
		})
	}
}

func TestEngineSchedulePauseAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inserted := 0
		engine := NewEngine(Config{}, scheduledAdapter{timestamps: []time.Time{time.Unix(0, 0), time.Unix(100, 0)}, insert: func(int) error { inserted++; return nil }})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		engine.running = true
		done := make(chan error, 1)
		go func() { done <- engine.replayOnce(ctx) }()
		synctest.Wait()
		pauseCtx, pauseCancel := context.WithTimeout(ctx, time.Second)
		defer pauseCancel()
		if err := engine.Pause(pauseCtx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Second)
		engine.Resume()
		synctest.Wait()
		if inserted != 1 {
			t.Fatalf("inserts=%d, want 1", inserted)
		}
		time.Sleep(99 * time.Second)
		if inserted != 1 {
			t.Fatal("pause consumed scheduled gap")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestEngineScheduleRestartsEachPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var times []time.Duration
		engine := NewEngine(Config{}, scheduledAdapter{timestamps: []time.Time{time.Unix(0, 0), time.Unix(20, 0)}, insert: func(int) error { times = append(times, time.Since(start)); return nil }})
		for range 2 {
			if err := engine.replayOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		for i, want := range []time.Duration{0, 20 * time.Second, 20 * time.Second, 40 * time.Second} {
			if times[i] != want {
				t.Fatalf("insert %d at %v, want %v", i, times[i], want)
			}
		}
	})
}

func TestEngineScheduleLagIncludesRecoveredRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		engine := NewEngine(Config{WorkerID: t.Name()}, scheduledAdapter{timestamps: []time.Time{time.Unix(0, 0), time.Unix(0, 0)}, insert: func(int) error {
			attempts++
			time.Sleep(time.Second)
			if attempts == 1 {
				return errors.New("database is locked")
			}
			return nil
		}})
		if err := engine.replayOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		labels := engine.metricLabels("scheduled")
		if got := testutil.ToFloat64(replayLagSeconds.WithLabelValues(labels...)); got != 2.05 {
			t.Fatalf("lag=%v, want 2.05", got)
		}
		metric := &dto.Metric{}
		histogram, err := replayOperationSeconds.GetMetricWithLabelValues(labels...)
		if err != nil {
			t.Fatal(err)
		}
		if err := histogram.(interface{ Write(*dto.Metric) error }).Write(metric); err != nil {
			t.Fatal(err)
		}
		if metric.GetHistogram().GetSampleCount() != 3 || metric.GetHistogram().GetSampleSum() != 3 {
			t.Fatalf("operation latency: %v", metric)
		}
		if got := testutil.ToFloat64(replayErrorsTotal.WithLabelValues(labels...)); got != 1 {
			t.Fatalf("errors=%v", got)
		}
	})
}

func TestEngineScheduleResumesRemainingGap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var second time.Duration
		engine := NewEngine(Config{WorkerID: t.Name()}, scheduledAdapter{timestamps: []time.Time{time.Time{}, time.Time{}.Add(10 * time.Second)}, insert: func(i int) error {
			if i == 1 {
				second = time.Since(start)
			}
			return nil
		}})
		engine.running = true
		done := make(chan error, 1)
		go func() { done <- engine.replayOnce(context.Background()) }()
		time.Sleep(3 * time.Second)
		if err := engine.Pause(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Second)
		engine.Resume()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if second != 30*time.Second {
			t.Fatalf("second insert at %v, want 30s", second)
		}
		if got := testutil.ToFloat64(replayLagSeconds.WithLabelValues(engine.metricLabels("scheduled")...)); got != 0 {
			t.Fatalf("lag=%v, want 0", got)
		}
	})
}
