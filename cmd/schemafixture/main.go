package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/corylanou/litestream-soak/internal/worker"
)

func main() {
	var options worker.SchemaFixtureOptions
	flag.StringVar(&options.S3Endpoint, "s3-endpoint", "", "optional S3-compatible endpoint")
	flag.StringVar(&options.S3Bucket, "s3-bucket", "", "existing fixture bucket")
	flag.StringVar(&options.S3Environment, "s3-environment", "", "emulator or provider")
	flag.StringVar(&options.Directory, "dir", "", "new fixture directory (must not exist)")
	flag.StringVar(&options.Binary, "litestream", "", "pinned Litestream binary")
	flag.StringVar(&options.SHA, "sha", "", "expected full Litestream SHA")
	flag.IntVar(&options.Rows, "rows", 4096, "rows in one growth transaction (multiple of four)")
	flag.IntVar(&options.PayloadBytes, "payload-bytes", 1024, "bytes per deterministic payload")
	flag.StringVar(&options.Vacuum, "vacuum", "vacuum", "vacuum or incremental")
	flag.Uint64Var(&options.MaxHeadroomBytes, "max-headroom-bytes", 0, "require available fixture filesystem bytes at or below this value; zero disables")
	timeout := flag.Duration("timeout", 10*time.Minute, "whole fixture deadline")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := worker.RunSchemaFixture(ctx, options)
	if err != nil {
		result.Error = err.Error()
	}
	if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
		os.Exit(1)
	}
	if err != nil || result.Verdict != "scenario_success" {
		os.Exit(1)
	}
}
