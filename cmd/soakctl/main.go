package main

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	workerAppName := envOrDefault("WORKER_APP_NAME", "litestream-soak")
	flyToken := os.Getenv("FLY_API_TOKEN")
	if flyToken == "" {
		slog.Error("FLY_API_TOKEN is required")
		os.Exit(1)
	}
	platformLogToken := envOrDefault("SOAK_PLATFORM_LOG_TOKEN", flyToken)

	dbPath := envOrDefault("DB_PATH", "/data/soakctl.db")
	s3Bucket := envOrDefault("S3_BUCKET", os.Getenv("BUCKET_NAME"))
	s3Endpoint := envOrDefault("S3_ENDPOINT", os.Getenv("AWS_ENDPOINT_URL_S3"))
	s3AccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	s3SecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	s3Region := os.Getenv("AWS_REGION")
	controlBaseURL := envOrDefault("CONTROL_BASE_URL", "https://litestream-soak-ctl.fly.dev")
	alertWebhookURL := os.Getenv("SOAK_ALERT_WEBHOOK_URL")
	alertWebhookToken := os.Getenv("SOAK_ALERT_WEBHOOK_BEARER_TOKEN")
	adminBearerToken := os.Getenv("SOAK_ADMIN_BEARER_TOKEN")
	basicAuthUsername := os.Getenv("SOAK_BASIC_AUTH_USERNAME")
	basicAuthPassword := os.Getenv("SOAK_BASIC_AUTH_PASSWORD")
	fleetEnabled := envOrDefault("SOAK_MAIN_FLEET_ENABLED", "false") == "true"
	dormancyEnabled := envOrDefault("SOAK_DORMANCY_ENABLED", "false") == "true"
	dormancyThreshold := durationEnvOrDefault("SOAK_DORMANCY_THRESHOLD", 24*time.Hour)
	dormancyInterval := durationEnvOrDefault("SOAK_DORMANCY_CHECK_INTERVAL", 10*time.Minute)
	dormancyMinFailures := intEnvOrDefault("SOAK_DORMANCY_MIN_FAILURES", 3)
	platformLogMonitorEnabled := envOrDefault("SOAK_PLATFORM_LOG_MONITOR_ENABLED", "true") == "true"
	platformLogPollInterval := durationEnvOrDefault("SOAK_PLATFORM_LOG_POLL_INTERVAL", time.Minute)
	volumeInventoryPollInterval := durationEnvOrDefault("SOAK_VOLUME_INVENTORY_POLL_INTERVAL", 10*time.Minute)
	unattachedVolumeGCEnabled := envOrDefault("SOAK_UNATTACHED_VOLUME_GC_ENABLED", "true") == "true"
	unattachedVolumeTTL := durationEnvOrDefault("SOAK_UNATTACHED_VOLUME_TTL", 2*time.Hour)
	if !unattachedVolumeGCEnabled {
		unattachedVolumeTTL = 0
	}
	webhookSecret := os.Getenv("GITHUB_WEBHOOK_SECRET")
	webhookDeployEnabled := envOrDefault("GITHUB_WEBHOOK_DEPLOY_ENABLED", "false") == "true"
	listenAddr := envOrDefault("LISTEN_ADDR", ":8080")

	db, err := model.Open(dbPath)
	if err != nil {
		slog.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	metrics := orchestrator.NewControlMetrics(db)
	alerts := orchestrator.NewAlertDispatcher(db, controlBaseURL, alertWebhookURL, alertWebhookToken)
	fly := flyapi.NewClient(workerAppName, flyToken)
	mgr := orchestrator.NewManager(fly, db, metrics, alerts, workerAppName, orchestrator.ReplicaConfig{
		Bucket:    s3Bucket,
		Endpoint:  s3Endpoint,
		AccessKey: s3AccessKey,
		SecretKey: s3SecretKey,
		Region:    s3Region,
	}, controlBaseURL, platformLogToken)
	deployer := orchestrator.NewDeployer(mgr, db, workerAppName, webhookDeployEnabled)
	webhookHandler := orchestrator.NewWebhookHandler(webhookSecret, deployer, webhookDeployEnabled)
	api := orchestrator.NewAPI(db, fly, metrics, alerts, mgr, deployer)

	mux := http.NewServeMux()
	mux.Handle("POST /webhooks/github", webhookHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	api.RegisterRoutes(mux)

	handler := http.Handler(mux)
	if (basicAuthUsername != "" && basicAuthPassword != "") || adminBearerToken != "" {
		handler = newAuthMiddleware(basicAuthUsername, basicAuthPassword, adminBearerToken)(handler)
	}

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
	if fleetEnabled {
		go mgr.RunFleetReconciler(ctx, orchestrator.DefaultMainFleet(), 10*time.Minute)
	}
	if dormancyEnabled {
		go mgr.RunDormancyLoop(ctx, orchestrator.DormancyPolicy{
			Threshold:     dormancyThreshold,
			CheckInterval: dormancyInterval,
			MinFailures:   dormancyMinFailures,
		})
	}
	if platformLogMonitorEnabled {
		go mgr.RunPlatformLogMonitor(ctx, platformLogPollInterval)
	}
	go mgr.RunVolumeInventoryMonitor(ctx, volumeInventoryPollInterval, unattachedVolumeTTL)

	slog.Info("soakctl starting",
		"listen", listenAddr,
		"worker_app", workerAppName,
		"s3_bucket", s3Bucket,
		"control_base_url", controlBaseURL,
		"alerts_enabled", alerts.Enabled(),
		"basic_auth_enabled", basicAuthUsername != "" && basicAuthPassword != "",
		"admin_bearer_enabled", adminBearerToken != "",
		"fleet_enabled", fleetEnabled,
		"dormancy_enabled", dormancyEnabled,
		"dormancy_threshold", dormancyThreshold,
		"dormancy_check_interval", dormancyInterval,
		"dormancy_min_failures", dormancyMinFailures,
		"platform_log_monitor_enabled", platformLogMonitorEnabled,
		"platform_log_poll_interval", platformLogPollInterval,
		"volume_inventory_poll_interval", volumeInventoryPollInterval,
		"unattached_volume_gc_enabled", unattachedVolumeGCEnabled,
		"unattached_volume_ttl", unattachedVolumeTTL,
		"platform_log_token_overridden", platformLogToken != flyToken,
		"webhook_deploy_enabled", webhookDeployEnabled,
	)

	server := &http.Server{Addr: listenAddr, Handler: handler}
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

func durationEnvOrDefault(key string, def time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return value
}

func intEnvOrDefault(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return value
}

func newAuthMiddleware(username, password, adminBearerToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipBasicAuth(r) {
				next.ServeHTTP(w, r)
				return
			}

			if strings.HasPrefix(r.URL.Path, "/api/admin/") {
				if isAdminBearerAuthorized(r, adminBearerToken) {
					next.ServeHTTP(w, r)
					return
				}
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			if isAdminBearerAuthorized(r, adminBearerToken) {
				next.ServeHTTP(w, r)
				return
			}

			if username == "" || password == "" {
				next.ServeHTTP(w, r)
				return
			}

			providedUser, providedPassword, ok := r.BasicAuth()
			if ok &&
				subtle.ConstantTimeCompare([]byte(providedUser), []byte(username)) == 1 &&
				subtle.ConstantTimeCompare([]byte(providedPassword), []byte(password)) == 1 {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("WWW-Authenticate", `Basic realm="litestream-soak-ctl"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		})
	}
}

func isAdminBearerAuthorized(r *http.Request, adminBearerToken string) bool {
	if adminBearerToken == "" || !strings.HasPrefix(r.URL.Path, "/api/admin/") {
		return false
	}
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return false
	}
	providedToken := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	return subtle.ConstantTimeCompare([]byte(providedToken), []byte(adminBearerToken)) == 1
}

func skipBasicAuth(r *http.Request) bool {
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
		(r.URL.Path == "/healthz" || r.URL.Path == "/metrics") {
		return true
	}
	if r.Method == http.MethodPost && r.URL.Path == "/webhooks/github" {
		return true
	}
	if r.Method != http.MethodPost {
		return false
	}
	return strings.HasPrefix(r.URL.Path, "/api/workers/") &&
		(strings.HasSuffix(r.URL.Path, "/heartbeat") || strings.HasSuffix(r.URL.Path, "/verifications") || strings.HasSuffix(r.URL.Path, "/events"))
}
