package orchestrator

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/reporting"
	"github.com/corylanou/litestream-soak/internal/workload"
)

type homePageData struct {
	GeneratedAt  time.Time
	Summary      homeSummary
	Diagnosis    diagnosisSnapshot
	Coverage     coverageSnapshot
	Spotlight    *FailureResponse
	FailureQueue []FailureResponse
	Workers      []homeWorker
	Events       []model.Event
}

type homeSummary struct {
	TotalWorkers     int
	HealthyWorkers   int
	AttentionWorkers int
	RecentFailures   int
}

type homeWorker struct {
	Worker                   model.Worker
	LatestVerification       *model.Verification
	Workload                 workload.Config
	RuntimeSnapshotStatus    string
	CurrentFailureStage      string
	CurrentFailureSignature  string
	CurrentProbableSubsystem string
}

type workerPageData struct {
	GeneratedAt time.Time
	Incident    *IncidentBundle
}

type helpPageData struct {
	GeneratedAt time.Time
	Diagnosis   diagnosisSnapshot
	Coverage    coverageSnapshot
	PromptModes []promptModeInfo
}

var uiTemplates = template.Must(template.New("ui").Funcs(template.FuncMap{
	"confidenceClass":   confidenceClass,
	"eventClass":        eventClass,
	"failureText":       failureText,
	"formatDuration":    formatDurationMS,
	"formatTime":        formatUITime,
	"formatTimePtr":     formatUITimePtr,
	"heartbeatClass":    heartbeatClass,
	"json":              mustJSON,
	"joinList":          strings.Join,
	"pathEscape":        url.PathEscape,
	"runtimeClass":      runtimeSnapshotClass,
	"runtimeLabel":      runtimeSnapshotLabel,
	"shorten":           shortenText,
	"statusClass":       statusClass,
	"timeAgo":           formatTimeAgoPtr,
	"timeAgoValue":      formatTimeAgo,
	"trimSHA":           trimSHA,
	"verificationClass": verificationClass,
	"verificationLabel": verificationLabel,
	"workerName":        workerName,
}).Parse(homePageTemplate + homeBodyTemplate + workerPageTemplate + helpPageTemplate))

func (a *API) handleHome(w http.ResponseWriter, r *http.Request) {
	data, err := a.buildHomePageData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "home", data)
}

func (a *API) handleHomePartial(w http.ResponseWriter, r *http.Request) {
	data, err := a.buildHomePageData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "home_body", data)
}

func (a *API) buildHomePageData() (homePageData, error) {
	summaries, err := a.listWorkerSummaries("")
	if err != nil {
		return homePageData{}, err
	}

	failures, err := a.db.ListRecentFailedVerifications(8)
	if err != nil {
		return homePageData{}, err
	}

	events, err := a.db.ListEvents(12)
	if err != nil {
		return homePageData{}, err
	}

	workerCards := make([]homeWorker, 0, len(summaries))
	summary := homeSummary{
		TotalWorkers:   len(summaries),
		RecentFailures: len(failures),
	}

	for _, workerSummary := range summaries {
		card := homeWorker{
			Worker:                   workerSummary.Worker,
			LatestVerification:       workerSummary.LastVerification,
			Workload:                 workerSummary.Workload,
			RuntimeSnapshotStatus:    workerSummary.RuntimeSnapshotStatus,
			CurrentFailureStage:      workerSummary.CurrentFailureStage,
			CurrentFailureSignature:  workerSummary.CurrentFailureSignature,
			CurrentProbableSubsystem: workerSummary.CurrentProbableSubsystem,
		}

		if workerSummary.Worker.Status == model.WorkerRunning {
			summary.HealthyWorkers++
		} else {
			summary.AttentionWorkers++
		}

		workerCards = append(workerCards, card)
	}

	sort.SliceStable(workerCards, func(i, j int) bool {
		left := workerCards[i]
		right := workerCards[j]
		if workerRank(left.Worker.Status) != workerRank(right.Worker.Status) {
			return workerRank(left.Worker.Status) < workerRank(right.Worker.Status)
		}
		return heartbeatUnix(left.Worker.LastHeartbeatAt) > heartbeatUnix(right.Worker.LastHeartbeatAt)
	})

	failureCards := make([]FailureResponse, 0, len(failures))
	for _, verification := range failures {
		card := FailureResponse{
			Verification:      verification,
			FailureStage:      inferFailureStage(&verification),
			FailureSignature:  inferFailureSignature(&verification),
			ProbableSubsystem: inferProbableSubsystem(inferFailureStage(&verification), inferFailureSignature(&verification)),
		}
		worker, err := a.db.GetWorker(verification.WorkerID)
		if err == nil {
			card.Worker = worker
		}
		failureCards = append(failureCards, card)
	}

	var spotlight *FailureResponse
	queue := make([]FailureResponse, 0)
	if len(failureCards) > 0 {
		spotlight = &failureCards[0]
		if len(failureCards) > 1 {
			queue = failureCards[1:]
		}
	}

	return homePageData{
		GeneratedAt:  time.Now().UTC(),
		Summary:      summary,
		Diagnosis:    buildDiagnosisSnapshot(summaries),
		Coverage:     buildCoverageSnapshot(summaries),
		Spotlight:    spotlight,
		FailureQueue: queue,
		Workers:      workerCards,
		Events:       events,
	}, nil
}

func (a *API) handleWorkerPage(w http.ResponseWriter, r *http.Request) {
	bundle, status, err := a.buildIncidentBundle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	renderHTML(w, "worker", workerPageData{
		GeneratedAt: time.Now().UTC(),
		Incident:    bundle,
	})
}

func (a *API) handleHelpPage(w http.ResponseWriter, r *http.Request) {
	summaries, err := a.listWorkerSummaries("")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderHTML(w, "help", helpPageData{
		GeneratedAt: time.Now().UTC(),
		Diagnosis:   buildDiagnosisSnapshot(summaries),
		Coverage:    buildCoverageSnapshot(summaries),
		PromptModes: buildPromptModes(string(promptModeTriage)),
	})
}

func renderHTML(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func formatUITime(value time.Time) string {
	return value.Local().Format("2006-01-02 15:04:05 MST")
}

func formatUITimePtr(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "never"
	}
	return formatUITime(*value)
}

func formatTimeAgo(value time.Time) string {
	if value.IsZero() {
		return "never"
	}

	delta := time.Since(value)
	if delta < 0 {
		delta = -delta
	}

	switch {
	case delta < time.Minute:
		return fmt.Sprintf("%ds ago", int(delta.Seconds()))
	case delta < time.Hour:
		return fmt.Sprintf("%dm ago", int(delta.Minutes()))
	case delta < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(delta.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(delta.Hours()/24))
	}
}

func formatTimeAgoPtr(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "never"
	}
	return formatTimeAgo(*value)
}

func trimSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func shortenText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit-1]) + "..."
}

func statusClass(value any) string {
	switch strings.ToLower(fmt.Sprint(value)) {
	case "running":
		return "status-good"
	case "degraded", "starting", "building", "pending":
		return "status-warn"
	case "failed", "stopped":
		return "status-bad"
	default:
		return "status-neutral"
	}
}

func confidenceClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return "status-good"
	case "medium":
		return "status-warn"
	default:
		return "status-neutral"
	}
}

func heartbeatClass(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "status-neutral"
	}
	age := time.Since(*value)
	switch {
	case age <= 45*time.Second:
		return "status-good"
	case age <= 2*time.Minute:
		return "status-warn"
	default:
		return "status-bad"
	}
}

func runtimeSnapshotClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case reporting.RuntimeSnapshotStatusHealthy:
		return "status-good"
	case reporting.RuntimeSnapshotStatusLegacy:
		return "status-warn"
	case reporting.RuntimeSnapshotStatusUnhealthy:
		return "status-bad"
	default:
		return "status-neutral"
	}
}

func runtimeSnapshotLabel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case reporting.RuntimeSnapshotStatusHealthy:
		return "snapshot ok"
	case reporting.RuntimeSnapshotStatusLegacy:
		return "legacy telemetry"
	case reporting.RuntimeSnapshotStatusUnhealthy:
		return "snapshot unhealthy"
	default:
		return "snapshot missing"
	}
}

func eventClass(value string) string {
	switch {
	case strings.Contains(value, "failed"):
		return "status-bad"
	case strings.Contains(value, "passed"):
		return "status-good"
	default:
		return "status-neutral"
	}
}

func verificationClass(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "status-neutral"
	}
	if verification.Passed {
		return "status-good"
	}
	return "status-bad"
}

func verificationLabel(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "no data"
	}
	if verification.Passed {
		return "pass"
	}
	return "fail"
}

func failureText(value any) string {
	verification := coerceVerification(value)
	if verification == nil {
		return "verification failed"
	}
	if strings.TrimSpace(verification.ErrorMessage) != "" {
		return verification.ErrorMessage
	}
	if strings.TrimSpace(verification.Status) != "" {
		return verification.Status
	}
	return "verification failed"
}

func workerName(worker *model.Worker, workerID string) string {
	if worker == nil {
		return workerID
	}
	if worker.Name != "" {
		return worker.Name
	}
	return worker.ID
}

func formatDurationMS(ms int) string {
	if ms <= 0 {
		return "n/a"
	}

	d := time.Duration(ms) * time.Millisecond
	if d < time.Second {
		return d.String()
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.Round(time.Second).String()
}

func coerceVerification(value any) *model.Verification {
	switch verification := value.(type) {
	case model.Verification:
		return &verification
	case *model.Verification:
		return verification
	default:
		return nil
	}
}

func workerRank(status model.WorkerStatus) int {
	switch status {
	case model.WorkerFailed:
		return 0
	case model.WorkerDegraded:
		return 1
	case model.WorkerStarting, model.WorkerBuilding, model.WorkerPending:
		return 2
	case model.WorkerRunning:
		return 3
	default:
		return 4
	}
}

func heartbeatUnix(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return value.Unix()
}

const homePageTemplate = `{{define "home"}}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Litestream Soak</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d1117;
      --surface: #161b22;
      --surface-raised: #1c2129;
      --border: #30363d;
      --border-subtle: #21262d;
      --text: #e6edf3;
      --muted: #8b949e;
      --faint: #484f58;
      --green: #3fb950;
      --green-dim: rgba(63,185,80,0.15);
      --amber: #d29922;
      --amber-dim: rgba(210,153,34,0.15);
      --red: #f85149;
      --red-dim: rgba(248,81,73,0.12);
      --blue: #58a6ff;
      --blue-dim: rgba(88,166,255,0.1);
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      --mono: "SF Mono", "Cascadia Code", "Fira Code", Consolas, "Liberation Mono", monospace;
      --radius: 6px;
      --radius-lg: 8px;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.5; }
    a { color: var(--blue); text-decoration: none; }
    a:hover { text-decoration: underline; }
    a:focus-visible, button:focus-visible, summary:focus-visible { outline: 2px solid var(--blue); outline-offset: 2px; }
    code, pre, .mono { font-family: var(--mono); font-size: 12px; }

    .shell { max-width: 1280px; margin: 0 auto; padding: 0 16px; }

    /* Top bar */
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 10px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
    .topbar-brand { font-weight: 600; font-size: 14px; letter-spacing: -0.01em; }
    .topbar-right { display: flex; align-items: center; gap: 12px; font-size: 12px; color: var(--muted); }
    .topbar-link { color: var(--muted); font-size: 12px; }
    .topbar-link:hover { color: var(--text); }
    .refresh-indicator { display: flex; align-items: center; gap: 6px; cursor: pointer; user-select: none; }
    .refresh-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--green); animation: pulse 2s ease-in-out infinite; }
    .refresh-dot.busy { background: var(--blue); animation: none; }
    .refresh-dot.paused { background: var(--muted); animation: none; }
    @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

    /* Status strip */
    .status-strip { display: flex; align-items: center; gap: 20px; padding: 10px 16px; background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); margin-bottom: 16px; font-size: 13px; }
    .status-strip .ss-label { color: var(--muted); }
    .status-strip .ss-value { font-weight: 600; font-variant-numeric: tabular-nums; }
    .ss-good { color: var(--green); }
    .ss-warn { color: var(--amber); }
    .ss-bad { color: var(--red); }

    /* Incident spotlight */
    .incident-alert { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; padding: 12px 16px; background: var(--red-dim); border: 1px solid rgba(248,81,73,0.3); border-radius: var(--radius-lg); margin-bottom: 16px; }
    .incident-alert-body { flex: 1; min-width: 0; }
    .incident-alert-title { font-weight: 600; font-size: 13px; margin-bottom: 4px; }
    .incident-alert-meta { color: var(--muted); font-size: 12px; }
    .incident-alert-actions { display: flex; gap: 8px; flex-shrink: 0; align-items: center; }
    .clear-banner { padding: 10px 16px; background: var(--green-dim); border: 1px solid rgba(63,185,80,0.25); border-radius: var(--radius-lg); margin-bottom: 16px; font-size: 13px; color: var(--green); font-weight: 500; }

    /* Buttons */
    .btn { display: inline-flex; align-items: center; gap: 6px; padding: 5px 12px; border-radius: var(--radius); border: 1px solid var(--border); background: var(--surface); color: var(--text); font-size: 12px; font-family: inherit; font-weight: 500; cursor: pointer; text-decoration: none; white-space: nowrap; }
    .btn:hover { background: var(--surface-raised); text-decoration: none; border-color: var(--faint); }
    .btn-primary { background: rgba(88,166,255,0.15); border-color: rgba(88,166,255,0.3); color: var(--blue); }
    .btn-primary:hover { background: rgba(88,166,255,0.25); }
    .btn-danger { background: var(--red-dim); border-color: rgba(248,81,73,0.3); color: var(--red); }

    /* Section headers */
    .section-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
    .section-title { font-size: 13px; font-weight: 600; color: var(--muted); letter-spacing: 0.02em; text-transform: uppercase; }

    /* Worker table */
    .worker-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .worker-table th { text-align: left; padding: 8px 12px; font-size: 11px; font-weight: 600; color: var(--faint); text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid var(--border); }
    .worker-table td { padding: 10px 12px; border-bottom: 1px solid var(--border-subtle); vertical-align: middle; }
    .worker-table tr.clickable-row { cursor: pointer; }
    .worker-table tr.clickable-row:hover td { background: var(--surface); }
    .worker-table tr.row-bad td { background: var(--red-dim); }
    .worker-table tr.row-bad:hover td { background: rgba(248,81,73,0.18); }
    .worker-table tr.row-warn td { background: var(--amber-dim); }
    .worker-table tr.row-warn:hover td { background: rgba(210,153,34,0.2); }

    /* Status dots */
    .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
    .dot-good { background: var(--green); }
    .dot-warn { background: var(--amber); }
    .dot-bad { background: var(--red); }
    .dot-neutral { background: var(--faint); }

    /* Badges */
    .badge { display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.02em; }
    .badge-good { background: var(--green-dim); color: var(--green); }
    .badge-warn { background: var(--amber-dim); color: var(--amber); }
    .badge-bad { background: var(--red-dim); color: var(--red); }
    .badge-neutral { background: rgba(139,148,158,0.15); color: var(--muted); }

    /* Panels */
    .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 14px 16px; }
    .guide-grid { display: grid; grid-template-columns: 1.35fr 1fr 1fr; gap: 16px; margin-bottom: 16px; }
    .guide-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 14px 16px; }
    .guide-card h2 { font-size: 15px; font-weight: 600; margin-bottom: 8px; }
    .guide-card p { color: var(--muted); font-size: 13px; }
    .guide-card ul { margin: 10px 0 0 18px; color: var(--muted); font-size: 13px; }
    .guide-card li + li { margin-top: 6px; }
    .guide-card .lead { color: var(--text); font-weight: 500; }
    .chip-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .chip { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 999px; background: var(--surface-raised); border: 1px solid var(--border-subtle); color: var(--text); font-size: 12px; }
    .chip strong { color: var(--blue); }
    .diag-meta { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .diag-meta .badge { text-transform: none; letter-spacing: 0; }
    .cluster-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin: 16px 0; }
    .cluster-card ul { margin: 10px 0 0 18px; }
    .cluster-card li + li { margin-top: 6px; }
    .cluster-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
    .cluster-title { font-size: 14px; font-weight: 600; }
    .cluster-summary { margin-top: 4px; color: var(--muted); font-size: 13px; }
    .cluster-workers { margin-top: 12px; color: var(--muted); font-size: 12px; line-height: 1.6; }
    .cluster-workers a { color: var(--blue); }

    /* Bottom grid */
    .bottom-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }

    /* Feed items */
    .feed-item { padding: 8px 0; border-bottom: 1px solid var(--border-subtle); font-size: 13px; }
    .feed-item:last-child { border-bottom: none; }
    .feed-row { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
    .feed-body { flex: 1; min-width: 0; }
    .feed-title { font-weight: 500; }
    .feed-meta { color: var(--muted); font-size: 12px; margin-top: 2px; }
    .feed-msg { color: var(--muted); font-size: 12px; margin-top: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .feed-time { color: var(--faint); font-size: 11px; flex-shrink: 0; white-space: nowrap; }
    .feed-actions { display: flex; gap: 8px; margin-top: 4px; }
    .feed-actions a { font-size: 12px; }

    /* Empty states */
    .empty { padding: 16px 0; color: var(--faint); font-size: 13px; text-align: center; }

    /* Footer */
    .footer { padding: 16px 0; margin-top: 24px; border-top: 1px solid var(--border-subtle); color: var(--faint); font-size: 11px; text-align: center; }

    @media (max-width: 900px) {
      .guide-grid { grid-template-columns: 1fr; }
      .bottom-grid { grid-template-columns: 1fr; }
      .status-strip { flex-wrap: wrap; gap: 10px 18px; }
      .incident-alert { flex-direction: column; }
    }
    @media (max-width: 640px) {
      .worker-table th:nth-child(n+5), .worker-table td:nth-child(n+5) { display: none; }
      .topbar { flex-direction: column; align-items: flex-start; gap: 6px; }
    }
  </style>
  <script>
    let refreshInterval = 10;
    let countdown = refreshInterval;
    let paused = false;
    let refreshing = false;
    let timer;

    function updateRefreshIndicator() {
      const el = document.getElementById("refresh-count");
      const dot = document.getElementById("refresh-dot");
      if (dot) {
        dot.classList.toggle("paused", paused);
        dot.classList.toggle("busy", refreshing);
      }
      if (!el) return;
      if (paused) {
        el.textContent = "paused";
        return;
      }
      if (refreshing) {
        el.textContent = "updating";
        return;
      }
      el.textContent = countdown + "s";
    }

    async function refreshHome() {
      if (paused || refreshing) return;
      refreshing = true;
      updateRefreshIndicator();

      try {
        const resp = await fetch(window.location.origin + "/ui/partials/home", {
          credentials: "same-origin",
          headers: { "X-Requested-With": "fetch" }
        });
        if (!resp.ok) return;
        const root = document.getElementById("home-live-root");
        if (!root) return;
        root.innerHTML = await resp.text();
      } finally {
        refreshing = false;
        countdown = refreshInterval;
        updateRefreshIndicator();
      }
    }

    function startCountdown() {
      updateRefreshIndicator();
      timer = setInterval(async function() {
        if (paused || refreshing) return;
        if (countdown > 0) {
          countdown--;
          updateRefreshIndicator();
          return;
        }
        await refreshHome();
      }, 1000);
    }

    function toggleRefresh() {
      paused = !paused;
      if (!paused && countdown <= 0) {
        countdown = refreshInterval;
      }
      updateRefreshIndicator();
    }

    function bindHomeInteractions() {
      var root = document.getElementById("home-live-root");
      if (!root) return;
      root.addEventListener("click", function(e) {
        if (e.target.closest("a,button,summary,textarea,details")) return;
        var row = e.target.closest(".clickable-row");
        if (!row) return;
        var href = row.dataset.href;
          if (href) window.location = href;
      });
    }

    document.addEventListener("DOMContentLoaded", function() {
      bindHomeInteractions();
      startCountdown();
    });
  </script>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="topbar-brand">Litestream Soak</div>
      <div class="topbar-right">
        <span class="refresh-indicator" onclick="toggleRefresh()" title="Click to pause/resume auto-refresh">
          <span class="refresh-dot" id="refresh-dot"></span>
          <span id="refresh-count">30s</span>
        </span>
        <a class="topbar-link" href="/ui/help">Help</a>
        <a class="topbar-link" href="/api/workers">API</a>
        <a class="topbar-link" href="/api/failures">Failures</a>
        <a class="topbar-link" href="/api/events">Events</a>
      </div>
    </div>
    <div id="home-live-root">{{template "home_body" .}}</div>
  </div>
</body>
</html>
{{end}}`

const homeBodyTemplate = `{{define "home_body"}}
    <div class="status-strip">
      <span><span class="ss-label">Workers</span> <span class="ss-value">{{.Summary.TotalWorkers}}</span></span>
      <span><span class="ss-label">Healthy</span> <span class="ss-value ss-good">{{.Summary.HealthyWorkers}}</span></span>
      <span><span class="ss-label">Attention</span> <span class="ss-value{{if gt .Summary.AttentionWorkers 0}} ss-warn{{end}}">{{.Summary.AttentionWorkers}}</span></span>
      <span><span class="ss-label">Failures</span> <span class="ss-value{{if gt .Summary.RecentFailures 0}} ss-bad{{end}}">{{.Summary.RecentFailures}}</span></span>
    </div>

    <div class="guide-grid">
      <div class="guide-card">
        <h2>Current Diagnosis</h2>
        <p class="lead">{{.Diagnosis.Headline}}</p>
        <p style="margin-top:8px;">{{.Diagnosis.Summary}}</p>
        {{if .Diagnosis.ProbableSubsystem}}
        <div class="diag-meta">
          {{if .Diagnosis.Confidence}}<span class="badge badge-{{if eq (confidenceClass .Diagnosis.Confidence) "status-good"}}good{{else if eq (confidenceClass .Diagnosis.Confidence) "status-warn"}}warn{{else}}neutral{{end}}">confidence: {{.Diagnosis.Confidence}}</span>{{end}}
          <span class="badge badge-warn">{{.Diagnosis.ProbableSubsystem}}</span>
          {{if .Diagnosis.DominantStage}}<span class="badge badge-neutral">stage: {{.Diagnosis.DominantStage}}</span>{{end}}
          {{if .Diagnosis.DominantSignature}}<span class="badge badge-bad">{{shorten .Diagnosis.DominantSignature 80}}</span>{{end}}
        </div>
        {{end}}
        {{if .Diagnosis.WhyLikely}}
        <ul>
          {{range .Diagnosis.WhyLikely}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        {{end}}
      </div>

      <div class="guide-card">
        <h2>Start Here</h2>
        <p>Use the control plane for incident context and Grafana for cluster shape.</p>
        <ul>
          <li>Open a worker marked <strong>degraded</strong>.</li>
          <li>Check failure stage, signature, and probable subsystem.</li>
          <li>Copy the AI prompt or incident JSON.</li>
          <li>Run the listed Fly triage commands before changing code.</li>
        </ul>
        <div class="chip-row">
          <a class="btn btn-primary" href="/ui/help">Operator Help</a>
          <a class="btn" href="/api/worker-summaries">Worker Summaries</a>
        </div>
      </div>

      <div class="guide-card">
        <h2>Coverage</h2>
        <p>The active fleet should span multiple load modes and data shapes.</p>
        {{if .Coverage.LoadModes}}
        <div class="chip-row">
          {{range .Coverage.LoadModes}}
          <span class="chip"><strong>{{.Count}}</strong> {{.Label}}</span>
          {{end}}
        </div>
        {{end}}
        {{if .Coverage.ReplayDatasets}}
        <div class="chip-row">
          {{range .Coverage.ReplayDatasets}}
          <span class="chip"><strong>{{.Count}}</strong> {{.Label}}</span>
          {{end}}
        </div>
        {{end}}
        {{if .Coverage.RuntimeStates}}
        <div class="chip-row">
          {{range .Coverage.RuntimeStates}}
          <span class="chip"><strong>{{.Count}}</strong> {{runtimeLabel .Label}}</span>
          {{end}}
        </div>
        {{end}}
      </div>
    </div>

    {{if .Diagnosis.Clusters}}
    <div class="section-head">
      <span class="section-title">Active Clusters</span>
    </div>
    <div class="cluster-grid">
      {{range .Diagnosis.Clusters}}
      <div class="panel cluster-card">
        <div class="cluster-head">
          <div>
            <div class="cluster-title">{{.Headline}}</div>
            <div class="cluster-summary">{{.Summary}}</div>
          </div>
          <span class="badge badge-{{if eq (confidenceClass .Confidence) "status-good"}}good{{else if eq (confidenceClass .Confidence) "status-warn"}}warn{{else}}neutral{{end}}">confidence: {{.Confidence}}</span>
        </div>
        <div class="diag-meta">
          <span class="badge badge-neutral">{{.WorkerCount}} workers</span>
          {{if .ProbableSubsystem}}<span class="badge badge-warn">{{.ProbableSubsystem}}</span>{{end}}
          {{if .Stage}}<span class="badge badge-neutral">stage: {{.Stage}}</span>{{end}}
          {{if .Signature}}<span class="badge badge-bad">{{shorten .Signature 80}}</span>{{end}}
        </div>
        {{if .WhyLikely}}
        <ul>
          {{range .WhyLikely}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        {{end}}
        {{if .Workers}}
        <div class="cluster-workers">
          {{range $index, $worker := .Workers}}{{if $index}}, {{end}}<a href="/ui/workers/{{pathEscape $worker.ID}}">{{$worker.Name}}</a>{{end}}
        </div>
        {{end}}
        <div class="feed-actions">
          <a href="/ui/workers/{{pathEscape .RepresentativeWorker.ID}}">Open {{.RepresentativeWorker.Name}}</a>
          <a href="/api/workers/{{pathEscape .RepresentativeWorker.ID}}/prompt?mode=triage">Prompt</a>
        </div>
      </div>
      {{end}}
    </div>
    {{end}}

    {{if .Spotlight}}
    <div class="incident-alert">
      <div class="incident-alert-body">
        <div class="incident-alert-title">{{workerName .Spotlight.Worker .Spotlight.Verification.WorkerID}}: {{shorten (failureText .Spotlight.Verification) 120}}</div>
        <div class="incident-alert-meta">{{timeAgoValue .Spotlight.Verification.StartedAt}} · {{.Spotlight.FailureStage}} · {{.Spotlight.ProbableSubsystem}} · {{formatDuration .Spotlight.Verification.DurationMS}}</div>
      </div>
      <div class="incident-alert-actions">
        <a class="btn btn-danger" href="/ui/workers/{{pathEscape .Spotlight.Verification.WorkerID}}">Open</a>
        <a class="btn" href="/api/workers/{{pathEscape .Spotlight.Verification.WorkerID}}/prompt">Copy Prompt</a>
      </div>
    </div>
    {{else}}
    <div class="clear-banner">All workers passing verification</div>
    {{end}}

    <div class="section-head">
      <span class="section-title">Fleet</span>
    </div>

    {{if .Workers}}
    <div class="panel" style="padding:0; overflow-x:auto;">
      <table class="worker-table">
        <thead>
          <tr>
            <th style="width:32px;"></th>
            <th>Name</th>
            <th>Status</th>
            <th>Heartbeat</th>
            <th>Last Check</th>
            <th>Profile</th>
            <th>Workload</th>
            <th>Telemetry</th>
            <th>Signal</th>
            <th style="width:80px;"></th>
          </tr>
        </thead>
        <tbody>
          {{range .Workers}}
          <tr class="clickable-row {{if eq (statusClass .Worker.Status) "status-bad"}}row-bad{{else if eq (statusClass .Worker.Status) "status-warn"}}row-warn{{end}}" data-href="/ui/workers/{{pathEscape .Worker.ID}}">
            <td><span class="dot dot-{{if eq (statusClass .Worker.Status) "status-good"}}good{{else if eq (statusClass .Worker.Status) "status-warn"}}warn{{else if eq (statusClass .Worker.Status) "status-bad"}}bad{{else}}neutral{{end}}"></span></td>
            <td><strong>{{.Worker.Name}}</strong></td>
            <td><span class="badge badge-{{if eq (statusClass .Worker.Status) "status-good"}}good{{else if eq (statusClass .Worker.Status) "status-warn"}}warn{{else if eq (statusClass .Worker.Status) "status-bad"}}bad{{else}}neutral{{end}}">{{.Worker.Status}}</span></td>
            <td><span class="{{heartbeatClass .Worker.LastHeartbeatAt}}">{{timeAgo .Worker.LastHeartbeatAt}}</span></td>
            <td>
              {{if .LatestVerification}}<span class="badge badge-{{if eq (verificationClass .LatestVerification) "status-good"}}good{{else if eq (verificationClass .LatestVerification) "status-bad"}}bad{{else}}neutral{{end}}">{{verificationLabel .LatestVerification}}</span> <span style="color:var(--muted); font-size:12px;">{{timeAgoValue .LatestVerification.StartedAt}}</span>
              {{else}}<span style="color:var(--faint);">none</span>{{end}}
            </td>
            <td>{{.Worker.ProfileName}}</td>
            <td>{{.Workload.MetricLoadMode}}{{if ne .Workload.MetricReplayDataset "none"}} · {{.Workload.MetricReplayDataset}}{{end}}</td>
            <td><span class="badge badge-{{if eq (runtimeClass .RuntimeSnapshotStatus) "status-good"}}good{{else if eq (runtimeClass .RuntimeSnapshotStatus) "status-warn"}}warn{{else if eq (runtimeClass .RuntimeSnapshotStatus) "status-bad"}}bad{{else}}neutral{{end}}">{{runtimeLabel .RuntimeSnapshotStatus}}</span></td>
            <td>
              {{if .CurrentFailureSignature}}
              <span class="badge badge-bad">{{shorten .CurrentFailureSignature 48}}</span>
              {{else}}
              <span class="badge badge-good">healthy</span>
              {{end}}
            </td>
            <td><a href="/api/workers/{{pathEscape .Worker.ID}}/prompt" onclick="event.stopPropagation();" class="btn" style="padding:2px 8px;">Prompt</a></td>
          </tr>
          {{end}}
        </tbody>
      </table>
    </div>
    {{else}}
    <div class="panel"><p class="empty">No workers have reported to the control plane yet.</p></div>
    {{end}}

    <div class="bottom-grid">
      <div>
        <div class="section-head">
          <span class="section-title">Failure Queue</span>
        </div>
        <div class="panel">
          {{if .FailureQueue}}
          {{range .FailureQueue}}
          <div class="feed-item">
            <div class="feed-row">
              <div class="feed-body">
                <div class="feed-title">{{workerName .Worker .Verification.WorkerID}}</div>
                <div class="feed-meta">{{.FailureStage}} · {{.ProbableSubsystem}} · {{formatDuration .Verification.DurationMS}}</div>
                <div class="feed-msg">{{shorten (failureText .Verification) 140}}</div>
                <div class="feed-actions">
                  <a href="/ui/workers/{{pathEscape .Verification.WorkerID}}">Open</a>
                  <a href="/api/workers/{{pathEscape .Verification.WorkerID}}/prompt">Prompt</a>
                </div>
              </div>
              <span class="feed-time">{{timeAgoValue .Verification.StartedAt}}</span>
            </div>
          </div>
          {{end}}
          {{else}}
          <p class="empty">No failed verifications queued</p>
          {{end}}
        </div>
      </div>

      <div>
        <div class="section-head">
          <span class="section-title">Event Feed</span>
        </div>
        <div class="panel">
          {{if .Events}}
          {{range .Events}}
          <div class="feed-item">
            <div class="feed-row">
              <div class="feed-body">
                <span class="badge badge-{{if eq (eventClass .EventType) "status-good"}}good{{else if eq (eventClass .EventType) "status-bad"}}bad{{else}}neutral{{end}}">{{.EventType}}</span>
                <div class="feed-msg">{{shorten .Message 140}}</div>
                {{if .WorkerID}}<div class="feed-meta"><code class="mono">{{.WorkerID}}</code></div>{{end}}
              </div>
              <span class="feed-time">{{timeAgoValue .CreatedAt}}</span>
            </div>
          </div>
          {{end}}
          {{else}}
          <p class="empty">No events recorded</p>
          {{end}}
        </div>
      </div>
    </div>

    <div class="footer">Generated {{formatTime .GeneratedAt}}</div>
{{end}}`

const workerPageTemplate = `{{define "worker"}}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Incident.Worker.Name}} · Litestream Soak</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d1117;
      --surface: #161b22;
      --surface-raised: #1c2129;
      --border: #30363d;
      --border-subtle: #21262d;
      --text: #e6edf3;
      --muted: #8b949e;
      --faint: #484f58;
      --green: #3fb950;
      --green-dim: rgba(63,185,80,0.15);
      --amber: #d29922;
      --amber-dim: rgba(210,153,34,0.15);
      --red: #f85149;
      --red-dim: rgba(248,81,73,0.12);
      --blue: #58a6ff;
      --blue-dim: rgba(88,166,255,0.1);
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      --mono: "SF Mono", "Cascadia Code", "Fira Code", Consolas, "Liberation Mono", monospace;
      --radius: 6px;
      --radius-lg: 8px;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.5; }
    a { color: var(--blue); text-decoration: none; }
    a:hover { text-decoration: underline; }
    a:focus-visible, button:focus-visible, summary:focus-visible { outline: 2px solid var(--blue); outline-offset: 2px; }
    code, pre, textarea, .mono { font-family: var(--mono); font-size: 12px; }
    h1, h2, h3, p { margin: 0; }

    .shell { max-width: 1280px; margin: 0 auto; padding: 0 16px; }

    /* Top bar */
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 10px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; font-size: 13px; }
    .topbar-nav { display: flex; align-items: center; gap: 6px; color: var(--muted); }
    .topbar-nav a { color: var(--muted); }
    .topbar-nav a:hover { color: var(--text); }
    .topbar-sep { color: var(--faint); }
    .topbar-right { display: flex; align-items: center; gap: 10px; }
    .topbar-link { color: var(--muted); font-size: 12px; }
    .topbar-link:hover { color: var(--text); }

    /* Buttons */
    .btn { display: inline-flex; align-items: center; gap: 6px; padding: 5px 12px; border-radius: var(--radius); border: 1px solid var(--border); background: var(--surface); color: var(--text); font-size: 12px; font-family: inherit; font-weight: 500; cursor: pointer; text-decoration: none; white-space: nowrap; }
    .btn:hover { background: var(--surface-raised); text-decoration: none; border-color: var(--faint); }
    .btn-primary { background: rgba(88,166,255,0.15); border-color: rgba(88,166,255,0.3); color: var(--blue); }
    .btn-primary:hover { background: rgba(88,166,255,0.25); }
    .btn-copy { background: rgba(63,185,80,0.15); border-color: rgba(63,185,80,0.3); color: var(--green); font-weight: 600; padding: 6px 16px; }
    .btn-copy:hover { background: rgba(63,185,80,0.25); }
    .btn-copy.copied { background: var(--green); color: var(--bg); border-color: var(--green); }
    .mode-btn.active { background: rgba(88,166,255,0.2); border-color: rgba(88,166,255,0.35); color: var(--blue); }

    /* Worker header */
    .worker-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 16px; }
    .worker-header-left { flex: 1; min-width: 0; }
    .worker-title { font-size: 22px; font-weight: 600; letter-spacing: -0.02em; margin-bottom: 8px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .worker-meta-strip { display: flex; flex-wrap: wrap; gap: 6px 16px; font-size: 12px; color: var(--muted); }
    .worker-meta-strip code { color: var(--text); }
    .meta-label { color: var(--faint); }

    /* Badges */
    .badge { display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.02em; }
    .badge-good { background: var(--green-dim); color: var(--green); }
    .badge-warn { background: var(--amber-dim); color: var(--amber); }
    .badge-bad { background: var(--red-dim); color: var(--red); }
    .badge-neutral { background: rgba(139,148,158,0.15); color: var(--muted); }

    /* Incident alert */
    .incident-alert { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; padding: 12px 16px; background: var(--red-dim); border: 1px solid rgba(248,81,73,0.3); border-radius: var(--radius-lg); margin-bottom: 16px; }
    .incident-alert-body { flex: 1; min-width: 0; }
    .incident-alert-title { font-weight: 600; font-size: 13px; margin-bottom: 4px; }
    .incident-alert-meta { color: var(--muted); font-size: 12px; }
    .guide-banner { display: grid; grid-template-columns: 1.15fr 1fr; gap: 16px; margin-bottom: 16px; }
    .guide-copy { color: var(--muted); font-size: 13px; }
    .guide-copy p + p { margin-top: 8px; }
    .guide-copy ul { margin: 10px 0 0 18px; }
    .guide-copy li + li { margin-top: 6px; }
    .guide-facts { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .chip-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .mode-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 10px; }
    .mode-summary { color: var(--muted); font-size: 12px; margin-bottom: 10px; }

    /* Layout */
    .layout { display: grid; grid-template-columns: 1fr 340px; gap: 16px; align-items: start; }
    .main-stack { display: grid; gap: 16px; }
    .sidebar { display: grid; gap: 16px; position: sticky; top: 16px; }

    /* Panels */
    .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 14px 16px; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
    .panel-title { font-size: 13px; font-weight: 600; color: var(--muted); letter-spacing: 0.02em; text-transform: uppercase; }

    /* Timeline */
    .tl-item { padding: 10px 0; border-bottom: 1px solid var(--border-subtle); }
    .tl-item:last-child { border-bottom: none; }
    .tl-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; margin-bottom: 6px; }
    .tl-left { display: flex; align-items: center; gap: 8px; }
    .tl-time { color: var(--faint); font-size: 11px; white-space: nowrap; }
    .tl-facts { display: flex; gap: 16px; font-size: 12px; color: var(--muted); }
    .tl-fact-label { color: var(--faint); }

    /* Details/disclosure */
    details { border-radius: var(--radius); border: 1px solid var(--border-subtle); background: var(--surface-raised); margin-top: 8px; }
    details + details { margin-top: 6px; }
    summary { cursor: pointer; list-style: none; padding: 8px 12px; font-size: 12px; font-weight: 500; color: var(--muted); }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: "\25B6\FE0E "; font-size: 10px; margin-right: 4px; }
    details[open] > summary::before { content: "\25BC\FE0E "; }
    .details-body { padding: 0 12px 12px; }
    pre { margin: 0; white-space: pre-wrap; word-break: break-word; overflow: auto; background: var(--bg); border: 1px solid var(--border-subtle); border-radius: var(--radius); padding: 10px 12px; font-size: 11px; line-height: 1.5; color: var(--muted); max-height: 400px; }
    textarea { width: 100%; min-height: 240px; resize: vertical; background: var(--bg); border: 1px solid var(--border-subtle); border-radius: var(--radius); padding: 10px 12px; font-size: 11px; line-height: 1.5; color: var(--text); }

    /* Key-value list */
    .kv-list { font-size: 13px; }
    .kv-row { display: flex; justify-content: space-between; gap: 8px; padding: 6px 0; border-bottom: 1px solid var(--border-subtle); }
    .kv-row:last-child { border-bottom: none; }
    .kv-label { color: var(--muted); flex-shrink: 0; }
    .kv-value { text-align: right; word-break: break-all; }
    .kv-value code { color: var(--text); }

    /* Dot */
    .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
    .dot-good { background: var(--green); }
    .dot-warn { background: var(--amber); }
    .dot-bad { background: var(--red); }
    .dot-neutral { background: var(--faint); }

    /* Empty */
    .empty { padding: 12px 0; color: var(--faint); font-size: 13px; }

    /* Footer */
    .footer { padding: 16px 0; margin-top: 24px; border-top: 1px solid var(--border-subtle); color: var(--faint); font-size: 11px; text-align: center; }

    @media (max-width: 960px) {
      .guide-banner { grid-template-columns: 1fr; }
      .layout { grid-template-columns: 1fr; }
      .sidebar { position: static; }
      .worker-header { flex-direction: column; }
    }
    @media (max-width: 640px) {
      .topbar { flex-direction: column; align-items: flex-start; gap: 6px; }
      .incident-alert { flex-direction: column; }
    }
  </style>
  <script>
    async function copyPrompt(btn) {
      var box = document.getElementById("prompt-box");
      try {
        await navigator.clipboard.writeText(box.value);
      } catch(e) {
        box.focus(); box.select(); document.execCommand("copy");
      }
      btn.textContent = "Copied"; btn.classList.add("copied");
      setTimeout(function() { btn.textContent = "Copy AI Prompt"; btn.classList.remove("copied"); }, 1800);
    }

    async function loadPromptMode(btn) {
      var mode = btn.dataset.mode;
      var summary = btn.dataset.summary;
      var label = btn.dataset.label;
      var box = document.getElementById("prompt-box");
      var endpoint = window.location.origin + "/api/workers/{{pathEscape .Incident.Worker.ID}}/prompt?mode=" + encodeURIComponent(mode);
      var resp = await fetch(endpoint);
      if (!resp.ok) return;
      box.value = await resp.text();
      document.getElementById("prompt-mode-label").textContent = label;
      document.getElementById("prompt-mode-summary").textContent = summary;
      document.querySelectorAll(".mode-btn").forEach(function(el) { el.classList.remove("active"); });
      btn.classList.add("active");
    }
  </script>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="topbar-nav">
        <a href="/ui">Litestream Soak</a>
        <span class="topbar-sep">/</span>
        <span>{{.Incident.Worker.Name}}</span>
      </div>
      <div class="topbar-right">
        <a class="topbar-link" href="/ui/help">Help</a>
        <a class="topbar-link" href="/api/workers/{{pathEscape .Incident.Worker.ID}}">JSON</a>
        <a class="topbar-link" href="/api/workers/{{pathEscape .Incident.Worker.ID}}/incident">Incident</a>
        <a class="topbar-link" href="/api/workers/{{pathEscape .Incident.Worker.ID}}/prompt">Prompt</a>
      </div>
    </div>

    <div class="worker-header">
      <div class="worker-header-left">
        <div class="worker-title">
          {{.Incident.Worker.Name}}
          <span class="badge badge-{{if eq (statusClass .Incident.Worker.Status) "status-good"}}good{{else if eq (statusClass .Incident.Worker.Status) "status-warn"}}warn{{else if eq (statusClass .Incident.Worker.Status) "status-bad"}}bad{{else}}neutral{{end}}">{{.Incident.Worker.Status}}</span>
          <span class="badge badge-{{if eq (heartbeatClass .Incident.Worker.LastHeartbeatAt) "status-good"}}good{{else if eq (heartbeatClass .Incident.Worker.LastHeartbeatAt) "status-warn"}}warn{{else if eq (heartbeatClass .Incident.Worker.LastHeartbeatAt) "status-bad"}}bad{{else}}neutral{{end}}">{{timeAgo .Incident.Worker.LastHeartbeatAt}}</span>
        </div>
        <div class="worker-meta-strip">
          <span><span class="meta-label">Profile</span> {{.Incident.Worker.ProfileName}}</span>
          <span><span class="meta-label">Source</span> {{.Incident.Worker.Source}}</span>
          <span><span class="meta-label">SHA</span> <code class="mono">{{trimSHA .Incident.Worker.GitSHA}}</code></span>
          {{if .Incident.Worker.FlyMachineID}}<span><span class="meta-label">Machine</span> <code class="mono">{{.Incident.Worker.FlyMachineID}}</code></span>{{end}}
        </div>
      </div>
      <button class="btn btn-copy" type="button" onclick="copyPrompt(this)">Copy AI Prompt</button>
    </div>

    {{if .Incident.LatestFailure}}
    <div class="incident-alert">
      <div class="incident-alert-body">
        <div class="incident-alert-title">{{failureText .Incident.LatestFailure}}</div>
        <div class="incident-alert-meta">{{formatTime .Incident.LatestFailure.StartedAt}} · {{.Incident.FailureStage}} · {{.Incident.Guide.ProbableSubsystem}} · {{formatDuration .Incident.LatestFailure.DurationMS}}</div>
      </div>
      <span class="badge badge-bad">failed</span>
    </div>
    {{end}}

    <div class="guide-banner">
      <div class="panel guide-copy">
        <div class="panel-head">
          <span class="panel-title">What To Do Next</span>
        </div>
        <p style="color:var(--text); font-weight:600;">{{.Incident.Guide.Headline}}</p>
        <p>{{.Incident.Guide.Summary}}</p>
        <div class="guide-facts">
          {{if .Incident.Guide.ProbableSubsystem}}<span class="badge badge-warn">{{.Incident.Guide.ProbableSubsystem}}</span>{{end}}
          {{if .Incident.FailureStage}}<span class="badge badge-neutral">stage: {{.Incident.FailureStage}}</span>{{end}}
          {{if .Incident.FailureSignature}}<span class="badge badge-bad">{{shorten .Incident.FailureSignature 96}}</span>{{end}}
          <span class="badge badge-{{if eq (runtimeClass .Incident.RuntimeSnapshotStatus) "status-good"}}good{{else if eq (runtimeClass .Incident.RuntimeSnapshotStatus) "status-warn"}}warn{{else if eq (runtimeClass .Incident.RuntimeSnapshotStatus) "status-bad"}}bad{{else}}neutral{{end}}">{{runtimeLabel .Incident.RuntimeSnapshotStatus}}</span>
        </div>
        {{if .Incident.Guide.WhyLikely}}
        <ul>
          {{range .Incident.Guide.WhyLikely}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        {{end}}
      </div>

      <div class="panel guide-copy">
        <div class="panel-head">
          <span class="panel-title">Operator Workflow</span>
        </div>
        <ul>
          {{range .Incident.Guide.NextSteps}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        <div class="chip-row" style="margin-top:12px;">
          <a class="btn btn-primary" href="/ui/help">Open Help</a>
          <a class="btn" href="/api/workers/{{pathEscape .Incident.Worker.ID}}/incident">Incident JSON</a>
        </div>
      </div>
    </div>

    <div class="layout">
      <div class="main-stack">
        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Verification Timeline</span>
            <span style="color:var(--faint); font-size:12px;">{{len .Incident.RecentVerifications}} recent</span>
          </div>
          {{if .Incident.RecentVerifications}}
          {{range .Incident.RecentVerifications}}
          <div class="tl-item">
            <div class="tl-head">
              <div class="tl-left">
                <span class="dot dot-{{if .Passed}}good{{else}}bad{{end}}"></span>
                <span class="badge badge-{{if .Passed}}good{{else}}bad{{end}}">{{if .Passed}}pass{{else}}fail{{end}}</span>
                <span style="font-size:13px;">{{.CheckType}}</span>
              </div>
              <span class="tl-time">{{timeAgoValue .StartedAt}}</span>
            </div>
            <div class="tl-facts">
              <span><span class="tl-fact-label">Duration</span> {{formatDuration .DurationMS}}</span>
              <span><span class="tl-fact-label">Status</span> {{.Status}}</span>
              {{if .CompletedAt}}<span><span class="tl-fact-label">Completed</span> {{formatTimePtr .CompletedAt}}</span>{{end}}
            </div>
            {{if .ErrorMessage}}
            <details>
              <summary>Error details</summary>
              <div class="details-body"><pre>{{.ErrorMessage}}</pre></div>
            </details>
            {{end}}
          </div>
          {{end}}
          {{else}}
          <p class="empty">No verification history recorded.</p>
          {{end}}
        </div>

        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Event Timeline</span>
            <span style="color:var(--faint); font-size:12px;">{{len .Incident.RecentEvents}} recent</span>
          </div>
          {{if .Incident.RecentEvents}}
          {{range .Incident.RecentEvents}}
          <div class="tl-item">
            <div class="tl-head">
              <div class="tl-left">
                <span class="badge badge-{{if eq (eventClass .EventType) "status-good"}}good{{else if eq (eventClass .EventType) "status-bad"}}bad{{else}}neutral{{end}}">{{.EventType}}</span>
              </div>
              <span class="tl-time">{{timeAgoValue .CreatedAt}}</span>
            </div>
            <div style="font-size:13px; color:var(--muted); margin-top:2px;">{{.Message}}</div>
            {{if .Details}}
            <details>
              <summary>Event details</summary>
              <div class="details-body"><pre>{{.Details}}</pre></div>
            </details>
            {{end}}
          </div>
          {{end}}
          {{else}}
          <p class="empty">No events recorded for this worker.</p>
          {{end}}
        </div>
      </div>

      <div class="sidebar">
        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">AI Debugging</span>
          </div>
          <div class="mode-row">
            {{range .Incident.PromptModes}}
            <button class="btn mode-btn{{if .Recommended}} active{{end}}" type="button" data-mode="{{.ID}}" data-label="{{.Label}}" data-summary="{{.Summary}}" onclick="loadPromptMode(this)">{{.Label}}</button>
            {{end}}
          </div>
          <div class="mode-summary"><strong id="prompt-mode-label">{{range .Incident.PromptModes}}{{if .Recommended}}{{.Label}}{{end}}{{end}}</strong> <span id="prompt-mode-summary">{{range .Incident.PromptModes}}{{if .Recommended}}{{.Summary}}{{end}}{{end}}</span></div>
          <button class="btn btn-copy" type="button" onclick="copyPrompt(this)" style="width:100%; justify-content:center; margin-bottom:10px;">Copy AI Prompt</button>
          <details>
            <summary>Inspect prompt text</summary>
            <div class="details-body">
              <textarea id="prompt-box" readonly>{{.Incident.Prompt}}</textarea>
            </div>
          </details>
        </div>

        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Workload Shape</span>
          </div>
          <div class="kv-list">
            <div class="kv-row"><span class="kv-label">Load mode</span><span class="kv-value">{{.Incident.Workload.MetricLoadMode}}</span></div>
            <div class="kv-row"><span class="kv-label">Replay dataset</span><span class="kv-value">{{.Incident.Workload.MetricReplayDataset}}</span></div>
            <div class="kv-row"><span class="kv-label">Pattern</span><span class="kv-value">{{.Incident.Workload.MetricPattern}}</span></div>
            <div class="kv-row"><span class="kv-label">Write rate</span><span class="kv-value">{{if .Incident.Workload.WriteRate}}{{.Incident.Workload.WriteRate}}{{else}}--{{end}}</span></div>
            <div class="kv-row"><span class="kv-label">Payload size</span><span class="kv-value">{{if .Incident.Workload.PayloadSize}}{{.Incident.Workload.PayloadSize}}{{else}}--{{end}}</span></div>
            <div class="kv-row"><span class="kv-label">Workers</span><span class="kv-value">{{if .Incident.Workload.Workers}}{{.Incident.Workload.Workers}}{{else}}--{{end}}</span></div>
            <div class="kv-row"><span class="kv-label">Replay speed</span><span class="kv-value">{{if .Incident.Workload.ReplaySpeed}}{{.Incident.Workload.ReplaySpeed}}{{else}}--{{end}}</span></div>
            <div class="kv-row"><span class="kv-label">Sync interval</span><span class="kv-value">{{if .Incident.Workload.SyncInterval}}{{.Incident.Workload.SyncInterval}}{{else}}--{{end}}</span></div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Worker Details</span>
          </div>
          <div class="kv-list">
            <div class="kv-row"><span class="kv-label">Worker ID</span><span class="kv-value"><code class="mono">{{.Incident.Worker.ID}}</code></span></div>
            <div class="kv-row"><span class="kv-label">Profile</span><span class="kv-value">{{.Incident.Worker.ProfileName}}</span></div>
            <div class="kv-row"><span class="kv-label">Source</span><span class="kv-value">{{.Incident.Worker.Source}}</span></div>
            <div class="kv-row"><span class="kv-label">Git SHA</span><span class="kv-value"><code class="mono">{{.Incident.Worker.GitSHA}}</code></span></div>
            <div class="kv-row"><span class="kv-label">Machine</span><span class="kv-value"><code class="mono">{{if .Incident.Worker.FlyMachineID}}{{.Incident.Worker.FlyMachineID}}{{else}}--{{end}}</code></span></div>
            <div class="kv-row"><span class="kv-label">Volume</span><span class="kv-value"><code class="mono">{{if .Incident.Worker.FlyVolumeID}}{{.Incident.Worker.FlyVolumeID}}{{else}}--{{end}}</code></span></div>
            <div class="kv-row"><span class="kv-label">Created</span><span class="kv-value">{{formatTime .Incident.Worker.CreatedAt}}</span></div>
            <div class="kv-row"><span class="kv-label">Last Heartbeat</span><span class="kv-value">{{formatTimePtr .Incident.Worker.LastHeartbeatAt}}</span></div>
            {{if .Incident.Worker.ErrorMessage}}<div class="kv-row"><span class="kv-label">Error</span><span class="kv-value" style="color:var(--red);">{{.Incident.Worker.ErrorMessage}}</span></div>{{end}}
          </div>
        </div>

        {{if .Incident.MachineError}}
        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Machine Error</span>
          </div>
          <div style="font-size:13px; color:var(--red);">{{.Incident.MachineError}}</div>
        </div>
        {{end}}

        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Machine</span>
          </div>
          {{if .Incident.Machine}}
          <details>
            <summary>Machine JSON</summary>
            <div class="details-body"><pre>{{json .Incident.Machine}}</pre></div>
          </details>
          {{else}}
          <p class="empty">{{if .Incident.MachineError}}See error above.{{else}}No machine data available.{{end}}</p>
          {{end}}
        </div>

        <div class="panel">
          <div class="panel-head">
            <span class="panel-title">Raw Evidence</span>
          </div>
          <details>
            <summary>Full incident JSON</summary>
            <div class="details-body"><pre>{{json .Incident}}</pre></div>
          </details>
        </div>
      </div>
    </div>

    <div class="footer">Generated {{formatTime .GeneratedAt}}</div>
  </div>
</body>
</html>
{{end}}`

const helpPageTemplate = `{{define "help"}}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Operator Help · Litestream Soak</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0d1117;
      --surface: #161b22;
      --surface-raised: #1c2129;
      --border: #30363d;
      --border-subtle: #21262d;
      --text: #e6edf3;
      --muted: #8b949e;
      --faint: #484f58;
      --green: #3fb950;
      --amber: #d29922;
      --red: #f85149;
      --blue: #58a6ff;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      --mono: "SF Mono", "Cascadia Code", "Fira Code", Consolas, "Liberation Mono", monospace;
      --radius: 8px;
    }
    *, *::before, *::after { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.5 var(--sans); }
    a { color: var(--blue); text-decoration: none; }
    a:hover { text-decoration: underline; }
    code { font-family: var(--mono); font-size: 12px; }
    .shell { max-width: 1240px; margin: 0 auto; padding: 0 16px 32px; }
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 0; border-bottom: 1px solid var(--border); margin-bottom: 20px; font-size: 13px; }
    .topbar-nav { display: flex; align-items: center; gap: 6px; color: var(--muted); }
    .topbar-nav a { color: var(--muted); }
    .hero { display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; margin-bottom: 16px; }
    .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px; }
    .panel h1, .panel h2, .panel h3 { margin: 0 0 8px; }
    .panel p { margin: 0; color: var(--muted); }
    .panel p + p { margin-top: 8px; }
    .panel ul, .panel ol { margin: 10px 0 0 18px; color: var(--muted); }
    .panel li + li { margin-top: 6px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .cluster-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin-bottom: 16px; }
    .cluster-card ul { margin: 10px 0 0 18px; }
    .cluster-card li + li { margin-top: 6px; }
    .cluster-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
    .cluster-title { font-size: 14px; font-weight: 600; }
    .cluster-summary { margin-top: 4px; color: var(--muted); font-size: 13px; }
    .badge-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .badge { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 999px; font-size: 12px; border: 1px solid var(--border-subtle); background: var(--surface-raised); }
    .badge strong { color: var(--blue); }
    .callout { color: var(--text); font-weight: 600; }
    .footer { padding-top: 18px; margin-top: 24px; border-top: 1px solid var(--border-subtle); color: var(--faint); font-size: 11px; text-align: center; }
    @media (max-width: 900px) {
      .hero, .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="topbar-nav">
        <a href="/ui">Litestream Soak</a>
        <span>/</span>
        <span>Operator Help</span>
      </div>
      <div class="topbar-nav">
        <a href="/api/diagnosis">Diagnosis JSON</a>
        <a href="/api/worker-summaries">Worker Summaries</a>
      </div>
    </div>

    <div class="hero">
      <div class="panel">
        <h1>How To Use This System</h1>
        <p class="callout">{{.Diagnosis.Headline}}</p>
        <p>{{.Diagnosis.Summary}}</p>
        <div class="badge-row">
          {{if .Diagnosis.Confidence}}<span class="badge">confidence: {{.Diagnosis.Confidence}}</span>{{end}}
          {{if .Diagnosis.ProbableSubsystem}}<span class="badge">{{.Diagnosis.ProbableSubsystem}}</span>{{end}}
          {{if .Diagnosis.DominantStage}}<span class="badge">stage: {{.Diagnosis.DominantStage}}</span>{{end}}
          {{if .Diagnosis.DominantSignature}}<span class="badge">{{shorten .Diagnosis.DominantSignature 96}}</span>{{end}}
        </div>
        {{if .Diagnosis.WhyLikely}}
        <ul>
          {{range .Diagnosis.WhyLikely}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        {{end}}
      </div>

      <div class="panel">
        <h2>Coverage Snapshot</h2>
        <p>The fleet should cover multiple load modes and replay shapes so failures are easier to cluster.</p>
        {{if .Coverage.LoadModes}}
        <div class="badge-row">
          {{range .Coverage.LoadModes}}
          <span class="badge"><strong>{{.Count}}</strong> {{.Label}}</span>
          {{end}}
        </div>
        {{end}}
        {{if .Coverage.ReplayDatasets}}
        <div class="badge-row">
          {{range .Coverage.ReplayDatasets}}
          <span class="badge"><strong>{{.Count}}</strong> {{.Label}}</span>
          {{end}}
        </div>
        {{end}}
        {{if .Coverage.RuntimeStates}}
        <div class="badge-row">
          {{range .Coverage.RuntimeStates}}
          <span class="badge"><strong>{{.Count}}</strong> {{runtimeLabel .Label}}</span>
          {{end}}
        </div>
        {{end}}
      </div>
    </div>

    {{if .Diagnosis.Clusters}}
    <div class="cluster-grid">
      {{range .Diagnosis.Clusters}}
      <div class="panel cluster-card">
        <div class="cluster-head">
          <div>
            <div class="cluster-title">{{.Headline}}</div>
            <div class="cluster-summary">{{.Summary}}</div>
          </div>
          <span class="badge">confidence: {{.Confidence}}</span>
        </div>
        <div class="badge-row">
          <span class="badge"><strong>{{.WorkerCount}}</strong> workers</span>
          {{if .ProbableSubsystem}}<span class="badge">{{.ProbableSubsystem}}</span>{{end}}
          {{if .Stage}}<span class="badge">stage: {{.Stage}}</span>{{end}}
        </div>
        {{if .WhyLikely}}
        <ul>
          {{range .WhyLikely}}
          <li>{{.}}</li>
          {{end}}
        </ul>
        {{end}}
      </div>
      {{end}}
    </div>
    {{end}}

    <div class="grid">
      <div class="panel">
        <h2>Operator Workflow</h2>
        <ol>
          <li>Start on <code>/ui</code> and find workers marked <code>degraded</code>.</li>
          <li>Open the failing worker page and record the failure stage, signature, and workload shape.</li>
          <li>Check the worker telemetry badge before trusting runtime fields. <code>legacy telemetry</code> means those fields are advisory until the fleet image is refreshed.</li>
          <li>Use Grafana to see whether the failure is isolated or clustered by profile or replay dataset.</li>
          <li>Copy the AI prompt or incident JSON, then run the listed Fly triage commands.</li>
        </ol>
      </div>

      <div class="panel">
        <h2>Control Plane vs Grafana</h2>
        <ul>
          <li>Use the control plane for worker identity, incident context, machine metadata, and next commands.</li>
          <li>Use Grafana for fleet posture, workload shape comparison, sync age drift, restart counters, and failure clustering.</li>
          <li>If the same signature appears across multiple profiles at once, suspect a shared subsystem first.</li>
        </ul>
      </div>

      <div class="panel">
        <h2>How To Use AI</h2>
        <p>Each worker page includes multiple prompt modes. Pick the prompt that matches your current question.</p>
        <ul>
          {{range .PromptModes}}
          <li><strong>{{.Label}}</strong>: {{.Summary}}</li>
          {{end}}
        </ul>
        <p>Ask the model to rank hypotheses, cite evidence from the incident bundle, and give exact next commands before proposing code changes.</p>
      </div>

      <div class="panel">
        <h2>Failure Families</h2>
        <ul>
          <li><strong>sync</strong>: Litestream control socket missing or timing out. Check <code>/data/litestream.sock</code>, the Litestream process, and restart behavior.</li>
          <li><strong>restore</strong>: replica object fetch, restore plan, or missing LTX trouble. Check restore logs and object-store behavior.</li>
          <li><strong>integrity_check</strong>: restore completed but validation failed. Focus on restore correctness and integrity output.</li>
        </ul>
      </div>
    </div>

    <div class="grid" style="margin-top:16px;">
      <div class="panel">
        <h2>Useful Endpoints</h2>
        <ul>
          <li><code>/ui</code></li>
          <li><code>/api/worker-summaries</code></li>
          <li><code>/api/failures</code></li>
          <li><code>/api/workers/{id}/incident</code></li>
          <li><code>/api/workers/{id}/prompt?mode=triage|litestream|harness</code></li>
        </ul>
      </div>

      <div class="panel">
        <h2>What Good Looks Like</h2>
        <ul>
          <li>You can explain the failing workload shape without SSHing anywhere.</li>
          <li>You can tell whether the issue is clustered or isolated before opening logs.</li>
          <li>You can hand the AI prompt bundle to another engineer and they can start immediately.</li>
        </ul>
      </div>
    </div>

    <div class="footer">Generated {{formatTime .GeneratedAt}}</div>
  </div>
</body>
</html>
{{end}}`
