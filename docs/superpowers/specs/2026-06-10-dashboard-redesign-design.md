# Dashboard Redesign: Glanceable Soak Control Plane

Date: 2026-06-10
Status: Approved (Cory, via interactive Q&A)
Branch: `redesign/glanceable-dashboard`

## Problem

The home and branch dashboards are text-dense: six equal-weight guide cards, long
prose summaries, and tables where a healthy row looks like a failing one. There are
no charts, sparklines, or trends, so digesting state requires reading everything.
The deep detail (worker page, incident JSON, prompts) is good and must be kept.

## Decisions (from Q&A)

- Primary question the dashboard answers in 5 seconds: **"What needs my attention?"**
- Charting: **Chart.js, vendored** into the binary via `go:embed` (no CDN at runtime —
  this tool is used during incidents).
- Scope: **all pages** — home, branch/source views, and worker detail.
- Visual direction: **keep the dark GitHub-style theme**, restructure the layout.

## Design principles (from research: NN/g, Grafana docs, Few/Tufte, status pages)

- Inverted pyramid: attention banner → KPI band → trends → tables → forensic feeds.
- Green is quiet, red is loud: healthy rows are visually recessive; only problems
  get saturated color and weight.
- Never color alone: pair status colors with icons/text (colorblind safety).
- Status-page idiom: per-worker win/loss tick bar of recent verifications.
- Progressive disclosure: prose guidance, rollout details, coverage, and feeds move
  below the fold or behind tabs/disclosures.

## Layout

### Home / branch dashboard (`/ui`, `/ui?source=...`)

1. **Attention banner** (full width): one row per actionable item — failure
   clusters (from diagnosis), stalled rollout (grace exceeded), regressing
   comparison, stale heartbeats. Each row: severity icon + headline + one-line
   cause + Open/Prompt buttons. Empty state: single muted "All clear — N workers
   passing" line. Main-fleet baseline failures render amber (expected baseline),
   not red.
2. **KPI band**: six stat tiles (big numerals, small label, delta where
   applicable): Fleet health %, Workers healthy/total, Pass rate 24h (+delta vs
   prior 24h), Failures 24h, Rollout updated/total + status, Stalest heartbeat.
3. **Trend row** (Chart.js, 3 charts, last 24h hourly buckets):
   - Pass rate % over time; on branch views overlays main vs branch.
   - Verification duration p50/p95.
   - Failures per hour (bar).
4. **Source tabs**: compact switcher (main | pr-NNN ...) with per-source
   attention counts; replaces the "Viewing" card.
5. **Comparison strip** (branch views / when comparison exists): verdict badge,
   pass/fail deltas, regressed/improved worker links, prompt/JSON actions.
6. **Fleet table**: dot + name, verification tick bar (last 20 checks), heartbeat
   age, last check, profile · workload (muted), signal. Failing rows sort first
   and get color; healthy rows stay neutral. Row click opens worker.
7. **Below the fold**: tabbed Failure Queue / Event Feed; collapsible details for
   Latest Rollout, Release Comparison, Diagnosis, Coverage (all existing content
   preserved, demoted).

### Worker page (`/ui/workers/{id}`)

- Keep structure (timeline, AI prompt sidebar, triage data) and all functionality.
- Replace stacked alert boxes with the same attention-banner component.
- Add: verification tick bar in the header area and a duration trend chart
  (Chart.js) above the verification timeline, built from RecentVerifications.
- Shared stylesheet replaces the inline duplicate CSS.

## Architecture

- `internal/orchestrator/assets/`: `chart.umd.min.js` (Chart.js v4, pinned),
  `dashboard.css` (extracted shared theme + new components), `dashboard.js`
  (auto-refresh + chart bootstrap). Served via `go:embed` static handler at
  `/assets/{file}` with cache headers.
- Chart data is embedded as JSON in `<script type="application/json">` inside the
  refreshable partial; `dashboard.js` (re)builds charts after each 10s swap.
  No new fetches on refresh.
- New model queries (`internal/model/verifications.go`):
  - `ListVerificationTicks(perWorker int)` — last N pass/fail ticks per worker
    (window function, one query).
  - `ListVerificationStatsSince(source string, since time.Time)` — slim rows
    (started_at, passed, status, duration_ms) joined to workers by source;
    bucketed into hourly series in Go (`ui_charts.go`, pure functions, unit
    tested).
- Tick bars are server-rendered HTML spans (no JS needed); Chart.js only powers
  the trend charts.
- `ui.go`: homePageData gains Attention, KPIs, ChartData, SourceTabs; existing
  fields stay (demoted sections reuse them).

## Error handling

- Chart data empty / Chart.js fails to load → trend row collapses gracefully
  (charts are progressive enhancement; the page works without JS as today).
- Sources with no verification history render empty tick bars ("no checks yet").

## Testing

- Unit tests for new model queries (existing `internal/model` test patterns) and
  for bucketing/aggregation functions.
- `go build ./... && go test ./...` clean.
- Manual browser verification with a seeded local SQLite DB: home (all-clear and
  failing states), branch view, worker page; screenshots captured.

## Out of scope

- Help page content rewrite (links/styles only if trivial).
- Grafana dashboards, API shapes, deploy pipeline (no deploy from this branch).
