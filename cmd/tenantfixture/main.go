package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/corylanou/litestream-soak/internal/worker"
)

func main() {
	var options worker.TenantLifecycleOptions
	flag.StringVar(&options.SHA, "sha", "", "full commit SHA reported by binary (required)")
	flag.StringVar(&options.Capabilities, "capabilities", "", "explicit capability contract: directory-v1 (required)")
	flag.StringVar(&options.Binary, "litestream", "", "path to pinned Litestream binary (required)")
	flag.StringVar(&options.Root, "output", "", "existing parent directory for fresh run artifacts (required)")
	flag.StringVar(&options.Mode, "mode", "watch", "discovery mode: watch or static")
	flag.IntVar(&options.Tenants, "tenants", 100, "tenant count, 2 through 1000; fleet tiers: 100, 500, 1000")
	flag.DurationVar(&options.SyncTimeout, "sync-timeout", time.Second, "per-request sync deadline (explicit comparison parameter)")
	flag.DurationVar(&options.Timeout, "timeout", 30*time.Second, "per-operation timeout, maximum 10m")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	report, err := worker.RunTenantLifecycle(ctx, options)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
