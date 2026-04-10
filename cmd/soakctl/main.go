package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/flyapi"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/orchestrator"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	appName := envOrDefault("FLY_APP_NAME", "litestream-soak")
	flyToken := os.Getenv("FLY_API_TOKEN")
	if flyToken == "" {
		slog.Error("FLY_API_TOKEN is required")
		os.Exit(1)
	}

	dbPath := envOrDefault("DB_PATH", "/data/soakctl.db")
	s3Bucket := envOrDefault("S3_BUCKET", os.Getenv("BUCKET_NAME"))
	s3Endpoint := envOrDefault("S3_ENDPOINT", os.Getenv("AWS_ENDPOINT_URL_S3"))
	webhookSecret := os.Getenv("GITHUB_WEBHOOK_SECRET")
	listenAddr := envOrDefault("LISTEN_ADDR", ":8080")

	db, err := model.Open(dbPath)
	if err != nil {
		slog.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	fly := flyapi.NewClient(appName, flyToken)
	mgr := orchestrator.NewManager(fly, db, appName, s3Bucket, s3Endpoint)
	deployer := orchestrator.NewDeployer(mgr, db, appName)
	webhookHandler := orchestrator.NewWebhookHandler(webhookSecret, mgr, deployer)

	mux := http.NewServeMux()
	mux.Handle("POST /webhooks/github", webhookHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	// API endpoints
	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, r *http.Request) {
		workers, err := db.ListWorkers("")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, workers)
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		events, err := db.ListEvents(50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, events)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Shutting down")
		cancel()
	}()

	go mgr.RunExpiryLoop(ctx)
	go mgr.RunHeartbeatMonitor(ctx, 5*time.Minute)

	slog.Info("soakctl starting",
		"listen", listenAddr,
		"app", appName,
		"s3_bucket", s3Bucket,
	)

	server := &http.Server{Addr: listenAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := encodeJSON(w, v); err != nil {
		slog.Error("Failed to encode JSON", "error", err)
	}
}

func encodeJSON(w http.ResponseWriter, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
