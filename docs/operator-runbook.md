# Litestream Soak Operator Runbook

## Purpose

This system exists to surface failures in Litestream soak testing quickly and
preserve enough context to investigate them. The goal is not to immediately fix
Litestream. The goal is to answer:

- Which workload shape is failing?
- What kind of failure is it?
- Is the failure isolated to one worker or clustered across profiles?
- What exact commands and evidence should an operator use next?

## Access

Control plane:

- `https://litestream-soak-ctl.fly.dev/ui`
- `https://litestream-soak-ctl.fly.dev/ui/help`

The control plane is protected with HTTP basic auth. Keep the username and
password in your local `.envrc`.

Deployment automation uses a separate bearer token for admin endpoints. Keep
that in GitHub Actions secrets as `SOAK_ADMIN_BEARER_TOKEN` instead of reusing
the UI basic-auth credentials. `/api/admin/*` is reserved for bearer-authenticated
admin automation only.

Worker reporting endpoints (`POST /api/workers/{id}/heartbeat`,
`/verifications`, and `/events`) require a third shared secret,
`SOAK_WORKER_TOKEN`, sent as `Authorization: Bearer <token>`. The control
plane refuses to start if it is unset, and rejects worker reports with `401`
if the token does not match. Set it once on the control plane app:

```bash
fly secrets set SOAK_WORKER_TOKEN=<random secret> -a litestream-soak-ctl
```

The orchestrator injects the token into every worker machine's environment
automatically, so workers need no manual configuration. Rotating the token
requires restarting workers so they pick up the new value.

Grafana:

- import `grafana/soak-overview-dashboard.json`
- import `grafana/soak-release-quality-dashboard.json`
- import `grafana/soak-source-compare-dashboard.json`
- import `grafana/soak-drilldown-dashboard.json`

## What To Look At First

### Control Plane Home

Start on `/ui`.

The home page is the fastest answer to "is anything wrong right now?" It shows:

- total workers, healthy workers, and workers needing attention
- a live-updating diagnosis summary that refreshes without a full page reload
- active failure clusters with confidence, affected workload shapes, and a representative worker
- an incident spotlight for the most urgent recent failure
- a worker table with status, heartbeat age, last check, profile, and telemetry health
- a failure queue with recent failed verifications
- an event feed

If the home page shows a worker as `degraded`, open that worker first.
If it shows a worker as `dormant`, the control plane has intentionally paused
that machine because the worker kept failing with the same signature for long
enough that continuing to run it was wasting compute.

### Worker Detail Page

Open `/ui/workers/{id}` for the failing worker.

This page is the incident page. It gives you:

- worker identity and workload shape
- last heartbeat and current status
- last verification result
- latest failure plus classified failure stage and signature
- latest Fly platform signal when one was detected from logs, such as `platform_oom`, `platform_disk_full`, `platform_restart`, or `platform_killed`
- recent verification history
- recent event history
- Fly machine metadata
- runtime snapshot status so you know whether DB status and sync-age fields are trustworthy
- dormancy metadata when the worker has been intentionally paused
- a copyable AI prompt bundle

### JSON Endpoints

These are the fastest machine-readable views:

- `/api/diagnosis`
- `/api/worker-summaries`
- `/api/failures`
- `/api/workers/{id}`
- `/api/workers/{id}/incident`
- `/api/workers/{id}/prompt`
- `/api/workers/{id}/debug-snapshot`

Use `/api/worker-summaries` to understand fleet posture in one request. It
includes workload config, last verification, latest failure, classified failure
stage/signature, and triage commands.

Use `/api/diagnosis` to inspect the fleet-level diagnosis, active clusters, and
coverage snapshot that drive the home page.

Use `/api/workers/{id}/incident` when you want the full bundle to inspect or
hand to an LLM.

Use `/api/workers/{id}/prompt` when you want a copy-paste triage prompt
immediately.

Use `/api/workers/{id}/debug-snapshot` when a worker has a recorded failure
debug bundle and you want only the captured evidence, not the full incident
context. This endpoint returns `404` when no failure snapshot has been captured.

Use `/api/events` for the default operator event feed. It now collapses repeated
platform signals into rolling incident rows so the feed stays readable. Use
`/api/events?raw=1` to inspect the uncollapsed event stream, or
`/api/events?worker_id={id}&raw=1` for a worker-specific raw view.

When a recent Fly platform signal exists, `/api/workers/{id}`,
`/api/workers/{id}/incident`, and `/api/workers/{id}/prompt` all include it.
Treat that as first-class evidence before assuming the verification failure
alone explains what happened.

Use `/ui/help` for the embedded operator guide and `/api/diagnosis` for the
live machine-readable diagnosis summary that powers the home page.

## Automatic Dormancy

The control plane can now act as a circuit breaker for sustained failures.

When enabled, it watches active workers across sources and looks for a
consecutive run of the same active failure signature. If that signature persists
long enough, the control plane archives failure evidence, moves the worker to
`dormant`, and stops the Fly Machine.

Interpret the new worker states this way:

- `degraded`: the worker is failing, but still running
- `dormant`: the worker was intentionally paused after sustained same-signature failures
- `probing`: the worker was resumed to test whether a new deploy or retry changed the result

Current dormancy behavior:

- compute is stopped, but the Fly volume is kept
- failure evidence is written to `/api/run-archives?type=failure`
- the control plane records `dormant_at`, `dormant_reason`, `dormant_signature`, and `resume_trigger`
- a new deploy wakes dormant main workers into `probing`
- if the probe fails, the worker returns to `dormant`
- if the probe passes, the worker returns to `running`

This is a cost-control feature, not a deletion policy. Dormant workers still
consume storage for their attached volumes.

Control it with these env vars on `litestream-soak-ctl`:

```bash
SOAK_DORMANCY_ENABLED=true
SOAK_DORMANCY_THRESHOLD=24h
SOAK_DORMANCY_CHECK_INTERVAL=10m
SOAK_DORMANCY_MIN_FAILURES=3
```

## Successful Run Teardown

Successful PR soaks can be archived and torn down automatically to avoid paying
for idle worker Machines, attached volumes, and stale replica prefixes after the
run has proven clean.

Current success teardown behavior:

- only sources matching `SOAK_SUCCESS_TEARDOWN_SOURCES` are eligible; the
  default is `pr-*`, so `main` is not auto-destroyed
- every worker must be on the latest deployment, running, runtime-healthy, and
  freshly heartbeating
- every worker must have a passing verification after the full soak window
- any failed verification in the deployment window blocks success teardown
- evidence is archived to `/api/run-archives?type=success` before deletion
- after archival, the control plane destroys worker Machines, volumes, and the
  worker replica prefix

Control it with these env vars on `litestream-soak-ctl`:

```bash
SOAK_SUCCESS_TEARDOWN_ENABLED=true
SOAK_SUCCESS_TEARDOWN_THRESHOLD=24h
SOAK_SUCCESS_TEARDOWN_CHECK_INTERVAL=10m
SOAK_SUCCESS_TEARDOWN_HEARTBEAT_STALE_AFTER=15m
SOAK_SUCCESS_TEARDOWN_SOURCES=pr-*
```

List archived runs with:

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/run-archives?source=pr-1228&type=success" | jq .
```

If you want to inspect dormant workers quickly:

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  https://litestream-soak-ctl.fly.dev/api/workers?status=dormant | jq .
```

If you want to manually wake dormant workers for a probe without waiting for a
new deploy:

```bash
curl -X POST -sS \
  -H "Authorization: Bearer $SOAK_ADMIN_BEARER_TOKEN" \
  "https://litestream-soak-ctl.fly.dev/api/admin/resume-dormant?source=main&trigger=manual_resume" | jq .
```

That resumes dormant workers using the current worker image already running in
Fly. If you want the probe to carry explicit version tracking, add
`&sha=<soak-git-sha>&litestream_sha=<upstream-litestream-sha>` to the request.

## Trusted Main Deployment Path

The trusted deploy path for `main` is now:

1. GitHub Actions builds the worker image with `flyctl deploy --build-only --push`.
2. GitHub Actions deploys the control plane when control-plane code changed.
3. GitHub Actions calls `POST /api/admin/deployments/ready`.
4. The control plane records the ready deployment, performs the rolling update,
   and resumes dormant workers into `probing`.

This matters because the worker fleet is a set of custom Fly Machines with
per-worker env and volume bindings. A plain `fly deploy` on the app is not the
same thing as a fleet-wide rolling update.

The control plane no longer assumes it should build worker images from inside
the running `soakctl` machine. Keep `GITHUB_WEBHOOK_DEPLOY_ENABLED=false` in
production unless you intentionally want the old in-process build path.

GitHub Actions needs these secrets and variables:

```bash
FLY_API_TOKEN=<fly token with deploy access>
SOAK_ADMIN_BEARER_TOKEN=<admin api token for soakctl>
SOAK_CONTROL_BASE_URL=https://litestream-soak-ctl.fly.dev
SOAK_LITESTREAM_SHA=<optional upstream litestream commit or tag>
```

Do not set `SOAK_LITESTREAM_SHA` to the soak repo commit. That value is only
for the upstream `benbjohnson/litestream` checkout performed inside
`Dockerfile.worker`.

The post-build handoff command is:

```bash
CONTROL_BASE_URL=https://litestream-soak-ctl.fly.dev \
SOAK_ADMIN_BEARER_TOKEN=... \
./scripts/notify-deployment-ready.sh \
  <soak-git-sha> \
  main \
  github_actions_main \
  <image-ref> \
  <upstream-litestream-sha>
```

If you need to test the same path manually without merging anything:

```bash
CONTROL_BASE_URL=https://litestream-soak-ctl.fly.dev \
SOAK_ADMIN_BEARER_TOKEN=... \
./scripts/notify-deployment-ready.sh \
  <soak-git-sha> \
  main \
  manual_test \
  <image-ref> \
  <upstream-litestream-sha>
```

Use the soak repo SHA for `<soak-git-sha>` and the actual commit being tested
from `github.com/benbjohnson/litestream` for `<upstream-litestream-sha>`. Those
are intentionally different fields in the control plane.

If GitHub Actions cannot deploy because the repo does not have
`FLY_API_TOKEN`, use this manual fallback after merging to `main`:

```bash
git checkout main
git pull

SHA=$(git rev-parse HEAD)
SHORT_SHA=$(git rev-parse --short=12 HEAD)
LITESTREAM_SHA="$(git ls-remote https://github.com/benbjohnson/litestream.git refs/heads/main | awk 'NR==1{print $1}')"

fly deploy \
  --config fly.toml \
  --app litestream-soak \
  --build-only \
  --push \
  --build-arg "LITESTREAM_SHA=${LITESTREAM_SHA}" \
  --image-label "sha-${SHORT_SHA}-ls-${LITESTREAM_SHA::12}"

CONTROL_BASE_URL=https://litestream-soak-ctl.fly.dev \
SOAK_ADMIN_BEARER_TOKEN=... \
./scripts/notify-deployment-ready.sh \
  "$SHA" \
  main \
  manual_main \
  "registry.fly.io/litestream-soak:sha-${SHORT_SHA}-ls-${LITESTREAM_SHA::12}" \
  "$LITESTREAM_SHA"
```

If you only need to wake dormant workers with the current image already running
in Fly:

```bash
curl -X POST -sS \
  -H "Authorization: Bearer $SOAK_ADMIN_BEARER_TOKEN" \
  "https://litestream-soak-ctl.fly.dev/api/admin/resume-dormant?source=main&trigger=manual_resume" | jq .
```

## Automatic Upstream Main Pickup

The soak system can now rebuild itself against the latest upstream Litestream
`main` without waiting for a change in this repo.

Workflow:

- `.github/workflows/sync-upstream-main.yml`

Behavior:

- runs on a schedule and via manual `workflow_dispatch`
- resolves the latest `github.com/benbjohnson/litestream` `refs/heads/main`
- checks the currently deployed `main` worker fleet from the public `/metrics`
- skips the build if the upstream Litestream SHA under test is already current
- otherwise builds a new worker image and notifies the control plane

Required GitHub settings:

```bash
FLY_API_TOKEN=<fly token with deploy access>
SOAK_ADMIN_BEARER_TOKEN=<admin api token for soakctl>
SOAK_CONTROL_BASE_URL=https://litestream-soak-ctl.fly.dev
```

Use `workflow_dispatch` with `force=true` if you want to rebuild against the
current upstream `main` SHA anyway.

## PR Soak Triggers

PR-specific soak testing now has three supported trigger paths.

### GitHub Actions Manual Trigger

Workflow:

- `.github/workflows/soak-pr.yml`

Inputs:

- `pr_number`
- optional `repo_full_name` (defaults to `benbjohnson/litestream`)
- optional `pr_sha`

This resolves the PR head SHA, builds a worker image against that Litestream
commit, then calls `/api/admin/deployments/ready` with `source=pr-<number>`.
The control plane creates or updates a PR-specific worker fleet under that
source automatically.

### Local CLI Trigger

If you want to start a PR soak from your machine without waiting for GitHub
Actions secrets or UI access:

```bash
SOAK_ADMIN_BEARER_TOKEN=... ./scripts/start-pr-soak.sh 1221
```

Optional arguments:

```bash
SOAK_ADMIN_BEARER_TOKEN=... ./scripts/start-pr-soak.sh 1221 benbjohnson/litestream
SOAK_ADMIN_BEARER_TOKEN=... ./scripts/start-pr-soak.sh 1221 benbjohnson/litestream <explicit-pr-head-sha>
```

That script:

- resolves the PR head SHA from GitHub unless you provide one
- builds a worker image with `LITESTREAM_SHA=<pr-head-sha>`
- notifies the control plane with `source=pr-1221`

### GitHub Label Or Cross-Repo Automation

This repo also accepts `repository_dispatch` with event type
`litestream_pr_soak_requested`.

That is the receiving side for a future label-based workflow in
`benbjohnson/litestream`. The upstream repo can react to a label like
`soak:test` and send:

```bash
gh api repos/corylanou/litestream-soak/dispatches \
  -X POST \
  -f event_type=litestream_pr_soak_requested \
  -F client_payload[pr_number]=1221 \
  -F client_payload[repo_full_name]=benbjohnson/litestream
```

The receiving workflow then builds and rolls the `pr-1221` soak fleet.

A ready-to-copy upstream workflow template lives at:

- `docs/examples/litestream-pr-soak-label.yml`

That template uses `pull_request_target`, validates the labeling actor against
an allowlist, then sends `repository_dispatch` into this repo.

Security model:

- control-plane admin actions require `SOAK_ADMIN_BEARER_TOKEN`
- GitHub workflow runs require access to this repo's Actions and secrets
- cross-repo dispatch requires a token with permission to dispatch into this repo
- PR soak requests are allowlisted to `benbjohnson/litestream` by default
- repository-dispatch PR soaks require an allowlisted triggering actor
- label-based triggering should be treated as a convenience signal, not the only authorization check

If you need to allow additional upstream repos later, set the repo variable:

```bash
SOAK_PR_REPO_ALLOWLIST=benbjohnson/litestream,owner/another-repo
```

Current default actor allowlist:

```bash
SOAK_PR_ACTOR_ALLOWLIST=benbjohnson,corylanou
```

If you want to override that list later, set:

```bash
SOAK_PR_ACTOR_ALLOWLIST=benbjohnson,corylanou,another-admin
SOAK_PR_LABEL_NAME=soak:test
```

Then have the upstream label workflow include `actor` and `label` in the
`repository_dispatch` payload. The receiving workflow in this repo will reject
requests from non-allowlisted actors or unexpected labels.

GitHub label permissions matter here:

- on organization repositories, anyone with `triage` access or higher can apply and dismiss labels
- on personal repositories, collaborators can manage labels

So if you need admin-only behavior, do not rely on label permissions alone.
Gate the dispatch on an actor allowlist, a restricted dispatch token, or both.

To verify that a merge or manual handoff actually propagated through the fleet:

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  https://litestream-soak-ctl.fly.dev/api/deployments/latest | jq .
```

That rollout view tells you:

- which SHA the control plane thinks is current
- how many workers are updated to that SHA
- how many are still probing after wake-up
- whether any workers fell back to `dormant` or `degraded`
- whether the rollout has moved beyond the 45-minute grace window

On the home page, the `Latest Rollout` card mirrors the same information for
faster review after a merge.

If the rollout is still `rolling_out`, `probing`, or `needs_attention` after
the 45-minute grace window, treat it as a stuck rollout until the affected
workers are explained.

## How The Control Plane Helps Debug

The control plane helps in four ways:

1. It keeps the latest worker workload next to the failure, so you can see
   whether the problem came from a synthetic, replay, or mixed worker.
2. It groups active failures into live clusters, so you can see whether the
   same signature is spreading across multiple workers or whether you are
   looking at multiple unrelated failure families.
3. It classifies the latest failure into a failure stage and failure signature.
4. It preserves recent verification history so you can tell whether the worker
   is stuck, flapping, or recovered.
5. It gives exact next-step commands for the affected Fly machine.
6. It tells you whether the worker is on healthy telemetry, legacy telemetry,
   or an unhealthy runtime snapshot before you trust runtime fields.
7. It records recent Fly platform signals so OOMs, disk pressure, and restart
   behavior can be investigated inside the same incident flow.

The incident prompt is built from the worker, workload, latest failure, recent
verifications, recent events, machine metadata, and triage commands in
`internal/orchestrator/api.go`.

Failed verifications and unexpected worker exits can attach a bounded
`failure_debug_snapshot`. The worker captures process state, FD counts, socket
summary, disk usage, cgroup state, Litestream child exit evidence, verifier
substep timings, recent process log tails, and object storage prefix summary
when available. The prompt includes this snapshot automatically and tells the
debugging agent to prioritize it over repeated final error messages.

The snapshot is failure-triggered and rate-limited for repeated same-signature
failures. It does not add steady-state compute. It adds small failure-time CPU
and disk reads, one bounded object-storage listing when S3/Tigris is configured,
and bounded control-plane event JSON storage.

## Platform Signals

The control plane now polls Fly app logs for each active worker and records
platform-level signals into the normal event stream. These appear on worker
detail pages, in incident bundles, and in AI prompt output.

Repeated platform log lines are collapsed in the default event feed and worker
incident views, but the latest raw log sample is preserved in event details and
the raw stream remains available through `/api/events?raw=1`.

If the control plane is using a deploy-scoped `FLY_API_TOKEN`, configure
`SOAK_PLATFORM_LOG_TOKEN` with a read-only org token so the Fly logs API can
be queried successfully.

Current platform event types:

- `platform_oom`: Fly reported an out-of-memory kill
- `platform_disk_full`: Fly or the process reported `no space left on device`
- `platform_restart`: Fly emitted a non-app restart or start event
- `platform_killed`: Fly logs reported a process kill

When one of these exists near a verification failure, debug the platform event
first. A `sync` failure caused by a missing Litestream socket after an OOM is
not the same class of problem as a clean restore or integrity failure.

Each worker page now exposes three AI prompt modes:

- `Fast triage`
- `Litestream deep dive`
- `Harness sanity check`

All prompt modes include the standard worker debug tools so another AI session
knows what it can ask an operator to run: `curl`, `jq`, `rg`, `procps`,
`iproute2`/`ss`, `sqlite3`, `/usr/bin/time`, `lsof`, `strace`, `file`,
`netcat-openbsd`, `dnsutils`, and `s3cmd`.

## How Grafana Helps Debug

Grafana is the fleet-level lens. Use it to answer "how broad is this?" before
you drill into one machine.

The dashboard is strongest at:

- fleet posture
- current verification freshness
- sync age drift
- worker restarts and Litestream restarts
- workload shape comparison
- disk pressure and local Litestream state growth
- current failure labels
- last failure labels even after a worker recovers

The key dashboard panels are:

- `Disk Pressure By Worker`
- `Volume GB By State`
- `Data Disk Used`
- `Local LTX State`
- `Storage Trend`
- `Fleet Last Failure Classes`
- `Fleet Last Failure Age`
- `Fleet Workload Shapes`
- `Selected Worker`
- `Current Failure Labels`

Use Grafana first when you need to know whether the problem is:

- profile-specific
- replay-dataset-specific
- high-write-load-specific
- spread across multiple workers at the same time

If four workers with different profiles all start failing on the same `/sync`
step within minutes, that points away from one bad dataset and toward a shared
Litestream or runtime issue.

## Telemetry Health And Fleet Drift

The control plane now classifies worker runtime telemetry as:

- `snapshot ok`
- `legacy telemetry`
- `snapshot unhealthy`
- `snapshot missing`

Interpret them this way:

- `snapshot ok`: the worker is reporting the new snapshot-health fields, so DB
  status, TXID, and sync-age fields are current enough to trust.
- `legacy telemetry`: the worker is still sending the older payload shape. The
  runtime fields may be stale and should be treated as advisory.
- `snapshot unhealthy`: the worker attempted to collect Litestream runtime
  stats and failed. The snapshot error is real signal.
- `snapshot missing`: the control plane has not received a usable runtime
  snapshot yet.

If several workers show `legacy telemetry` while a newer worker shows
`snapshot ok`, the fleet is likely split across worker images. `fly deploy`
updates the default `app` machine, but it does not automatically refresh every
worker machine in this fleet layout.

Use this dry-run command to inspect fleet image drift:

```bash
make refresh-worker-fleet
```

To execute the refresh:

```bash
RUN=1 make refresh-worker-fleet
```

That command updates non-`app` worker machines to the newest image discovered
from the app.

## Standard Triage Flow

1. Open the control plane home page.
2. Find the workers marked `degraded`.
3. Open a failing worker page.
4. Record:
   - worker ID
   - profile name
   - load mode
   - replay dataset, if any
   - failure stage
   - failure signature
   - telemetry status
   - last heartbeat
   - last verification duration
5. Compare against Grafana:
   - is the failure clustered by profile?
   - is it clustered by replay dataset?
   - are sync age or restart counters also moving?
   - are the affected workers on `legacy telemetry` or `snapshot ok`?
6. Copy the prompt bundle or incident JSON.
7. Run the triage commands from the worker page or API response.

## Standard Triage Commands

Every worker summary and incident bundle includes commands like:

```bash
export SOAK_BASIC_AUTH_USERNAME=...
export SOAK_BASIC_AUTH_PASSWORD=...
fly machine status <machine-id> -a litestream-soak
fly logs -a litestream-soak -i <machine-id>
fly ssh console -a litestream-soak
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  https://litestream-soak-ctl.fly.dev/api/workers/<worker-id>/incident | jq .
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  https://litestream-soak-ctl.fly.dev/api/diagnosis | jq .
```

Inside the worker machine, these checks are usually the most useful:

```bash
ps aux | rg litestream
ss -xlpn | rg litestream.sock
lsof /data/test.db /data/test.db-wal /data/litestream.sock
df -h /data
du -sh /data/test.db /data/test.db-wal /data/.test.db-litestream 2>/dev/null
ls -lah /data
ls -lah /data/litestream.sock
tail -n 50 /data/verification.log
cat /data/litestream.yml
sqlite3 /data/test.db 'pragma wal_checkpoint(passive);'
litestream ltx -level all /data/test.db
/usr/bin/time -p curl --unix-socket /data/litestream.sock \
  'http://localhost/txid?path=/data/test.db'
s3cmd --access_key="$AWS_ACCESS_KEY_ID" --secret_key="$AWS_SECRET_ACCESS_KEY" \
  --host=fly.storage.tigris.dev --host-bucket='%(bucket)s.fly.storage.tigris.dev' \
  --region="$AWS_REGION" ls "s3://$S3_BUCKET/$S3_PATH/"
```

The standard worker image includes `curl`, `jq`, `ripgrep`, `procps`,
`iproute2`, `sqlite3`, `time`, `lsof`, `strace`, `file`, `netcat-openbsd`,
`dnsutils`, and `s3cmd` so those commands work without a separate debug image.

## How To Debug Litestream Specifically

The verifier flow is:

1. pause the load generator
2. checkpoint SQLite
3. wait for Litestream `/sync`
4. run `litestream-test validate`
5. resume the load generator

That flow is implemented in `internal/worker/verifier.go`.

Use the failure stage to narrow the problem:

### `restore`

Typical meaning:

- replica fetch failed
- S3 object lookup timed out
- restore plan failed
- LTX file missing

Check:

- whether multiple workers are failing with S3 timeouts
- whether failures are isolated to one worker prefix in the bucket
- worker logs around restore
- the incident bundle for the exact restore error

This usually points to the replication or object-fetch path, not the workload
generator itself.

### `integrity_check`

Typical meaning:

- restore completed, but the restored DB failed validation
- SQLite index mismatch
- content divergence after restore

Check:

- whether restore succeeded before validation failed
- worker verification history for repeated integrity failures
- the exact validation output in the incident bundle
- DB size, WAL size, and TXID context

This is the strongest signal that Litestream or restore correctness may be the
issue rather than Fly runtime.

### `/sync` socket failures

Typical meaning:

- Litestream is not listening on `/data/litestream.sock`
- Litestream is unhealthy or restarting
- the sync call is timing out under load

Check:

- `ls -lah /data/litestream.sock`
- `ps aux | rg litestream`
- worker logs for Litestream startup or crash messages
- whether this failure hits multiple high-load workers at once
- restart counters and sync-age panels in Grafana

If several workers fail on `/sync` at the same time, treat that as a shared
Litestream/runtime condition first, not a bad replay dataset.

## What The System Captures For You

Workers send this to the control plane:

- worker ID, machine ID, region, profile, and source
- DB size and WAL size
- DB TXID
- DB status
- last sync age
- Litestream uptime
- whether the Litestream runtime snapshot is healthy
- the latest Litestream runtime snapshot error when the control socket polls fail
- verification status, duration, summary, and error text

The control plane also normalizes older worker payloads so legacy telemetry is
flagged explicitly instead of being silently treated as current.

That reporting contract lives in `internal/reporting/types.go`.

## Current Operator Mental Model

Use the control plane to answer:

- What failed most recently?
- On which exact worker?
- Under what workload shape?
- What command should I run next?

Use Grafana to answer:

- How broad is the failure?
- Is it current or just historical?
- Does it correlate with one workload family or resource pattern?

Use the incident bundle to answer:

- What do I hand to an AI model or engineer so they can start investigating
  without re-collecting context?

## Onboarding Checklist

1. Get control-plane credentials and store them locally in `.envrc`.
2. Import the Grafana dashboard from `grafana/soak-drilldown-dashboard.json`.
3. Learn the three main endpoints:
   - `/ui`
   - `/api/worker-summaries`
   - `/api/workers/{id}/incident`
4. Learn the three main failure families:
   - restore failures
   - integrity-check failures
   - `/sync` socket or timeout failures
5. Learn the telemetry status badges:
   - `snapshot ok`
   - `legacy telemetry`
   - `snapshot unhealthy`
   - `snapshot missing`
6. Practice one investigation with:
   - control plane
   - Grafana
   - Fly logs
   - worker incident bundle

## Next Documentation To Add

Useful follow-up docs:

- a short setup guide for adding a new replay dataset
- a failure-signature catalog with examples
- a Grafana panel guide with screenshots
- a "known failure shapes" page that maps signature to likely subsystem
