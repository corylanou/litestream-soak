package churn

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestScheduleDeterministicAndHot(t *testing.T) {
	cfg := Config{Slots: 100, HotPercent: 100, Workers: 4, Rate: 100, PayloadSize: 32, Seed: 7}
	a := New("queue", cfg, nil)
	b := New("queue", cfg, nil)
	for i := uint64(0); i < 1000; i++ {
		x := a.operation(i)
		y := b.operation(i)
		if !reflect.DeepEqual(x, y) || x.key >= 10 || x.key < 0 {
			t.Fatalf("invalid deterministic hot operation: %+v %+v", x, y)
		}
	}
}

func TestEnginePauseAndStop(t *testing.T) {
	for _, mode := range []string{"queue", "cache"} {
		t.Run(mode, func(t *testing.T) {
			db := testDB(t)
			observed := make(chan struct{}, 100)
			e := New(mode, Config{Slots: 4, Workers: 3, Rate: 1000, PayloadSize: 8}, func(op string, n int64, d time.Duration, err error) {
				select {
				case observed <- struct{}{}:
				default:
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := e.Start(ctx, db); err != nil {
				t.Fatal(err)
			}
			defer e.Stop()
			select {
			case <-observed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := e.Pause(ctx); err != nil {
				t.Fatal(err)
			}
			if err := Validate(ctx, db, mode, 4); err != nil {
				t.Fatal(err)
			}
			for len(observed) > 0 {
				<-observed
			}
			e.Resume()
			select {
			case <-observed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			e.Stop()
		})
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{{}, {Slots: 1, Workers: 0, Rate: 1, PayloadSize: 1}, {Slots: 1, Workers: 1, Rate: 0, PayloadSize: 1}, {Slots: 1, Workers: 1, Rate: 1, PayloadSize: 1, HotPercent: 101}} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
}

func TestPauseWaitsForConcurrentAttempts(t *testing.T) {
	db := testDB(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	e := New("cache", Config{Slots: 4, Workers: 2, Rate: 1000, PayloadSize: 8}, func(string, int64, time.Duration, error) { entered <- struct{}{}; <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Start(ctx, db); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			t.Fatal(ctx.Err())
		}
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	err := e.Pause(canceled)
	close(release)
	if err != context.Canceled {
		t.Fatalf("pause returned before attempts finished: %v", err)
	}
	if err := e.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Validate(ctx, db, "cache", 4); err != nil {
		t.Fatal(err)
	}
}

func TestEngineRejectsInvalidStart(t *testing.T) {
	db := testDB(t)
	cfg := Config{Slots: 4, Workers: 1, Rate: 1, PayloadSize: 8}
	if err := New("unknown", cfg, nil).Start(context.Background(), db); err == nil {
		t.Fatal("accepted unknown mode")
	}
	if err := New("queue", Config{}, nil).Start(context.Background(), db); err == nil {
		t.Fatal("accepted invalid config")
	}
	e := New("queue", cfg, nil)
	if err := e.Start(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	defer e.Stop()
	if err := e.Start(context.Background(), db); err == nil {
		t.Fatal("accepted double start")
	}
}
