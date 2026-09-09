package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/corylanou/litestream-soak/internal/recovery"
)

func main() {
	var cfg recovery.Config
	flag.StringVar(&cfg.Binary, "binary", "", "local Litestream executable")
	flag.StringVar(&cfg.SHA256, "sha256", "", "required executable SHA-256 pin")
	flag.StringVar(&cfg.Output, "output", "", "new local evidence directory")
	flag.DurationVar(&cfg.Window, "window", 30*time.Second, "bounded scenario window")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := recovery.Run(ctx, cfg)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
		fmt.Fprintln(os.Stderr, encodeErr)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if result.Status != "passed" {
		os.Exit(1)
	}
}
