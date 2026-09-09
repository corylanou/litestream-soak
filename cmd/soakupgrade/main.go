package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/corylanou/litestream-soak/internal/upgrade"
)

func main() {
	var cfg upgrade.Config
	flag.StringVar(&cfg.Fixture, "fixture", "", "sealed fixture directory from an earlier run")
	flag.StringVar(&cfg.Mode, "mode", "fresh-start", "fresh-start or persistent-upgrade")
	flag.StringVar(&cfg.Output, "output", "", "new directory for all retained artifacts")
	flag.StringVar(&cfg.Baseline.Path, "baseline", "", "baseline binary path")
	flag.StringVar(&cfg.Baseline.SHA256, "baseline-sha256", "", "expected baseline binary SHA-256")
	flag.StringVar(&cfg.Candidate.Path, "candidate", "", "candidate binary path")
	flag.StringVar(&cfg.Candidate.SHA256, "candidate-sha256", "", "expected candidate binary SHA-256")
	flag.StringVar(&cfg.Transition, "transition", "", "explicit supported format declaration: ltx-v1")
	flag.BoolVar(&cfg.Rollback, "rollback-supported", false, "explicitly declare same-format rollback support")
	flag.DurationVar(&cfg.AgeTimeout, "age-timeout", time.Minute, "maximum fixture aging window")
	flag.DurationVar(&cfg.ContinueFor, "continue-for", 5*time.Second, "continuation window per arm")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	result, err := upgrade.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(result.Status)
	if result.Status != "passed" {
		os.Exit(1)
	}
}
