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

Rotate the basic-auth password with:

```bash
fly secrets set SOAK_BASIC_AUTH_PASSWORD="$(openssl rand -hex 16)" -a litestream-soak-ctl
```

This triggers a rolling restart of the control plane only; the worker fleet is
unaffected. Update the value in your local `.envrc` to match.

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
automatically, so workers need no manual configuration. The env is fixed at
machine creation, so rotating the token requires recreating worker machines
(for example via a fleet rollout) — restarting an existing machine keeps the
old value and its reports will be rejected with `401`.

The token is fleet-wide: it authenticates that a report comes from *a* worker,
not from a specific one, and it is stored in plaintext in each worker
machine's config (like the replica credentials), so treat read access to the
worker app as equivalent to holding the token.

First rollout ordering matters: worker machines created before this control
plane version have no token in their env, and there is no grace mode.

1. `fly secrets set SOAK_WORKER_TOKEN=<random secret> -a litestream-soak-ctl`
   (the control plane refuses to start without it)
2. Deploy the control plane
3. Recreate/roll all worker machines immediately — until then the existing
   fleet's heartbeats, verifications, and events are rejected with `401`

Grafana:

- import `grafana/soak-overview-dashboard.json`
- import `grafana/soak-release-quality-dashboard.json`
- import `grafana/soak-source-compare-dashboard.json`
- import `grafana/soak-drilldown-dashboard.json`

## Machine diagnostic credential exposure

Issue #260 records a confirmed exposure on control image
`193dc9f2dd63600af2246e00e089683adeb87220`: authenticated incident output
included storage credentials and the worker reporting token in machine
configuration. The same representation reached worker details, generated
prompts, and the worker UI. A diagnostic capture was accidentally printed;
coordinator captures were redacted. No credential values belong in issues,
PRs, logs, prompts, screenshots, or this runbook.

Machine diagnostics now use an explicit metadata allowlist: machine and
instance identity, name, state, region, image reference, resource allocation,
volume mounts, timestamps, and platform events. Environment variables,
services, and metrics configuration are omitted. Provider error bodies are
also omitted from error strings and JSON because they can echo configuration
and otherwise persist in worker status, events, and archives. Internal retry
classification can still inspect the private provider body. Operational Fly requests
retain their configuration internally. This does not erase older captures or
sanitize arbitrary application log messages.

Preserve failure evidence using redacted copies with the machine environment
removed, retaining timestamps, run/build identity, volume identity, and
verification details. Restrict access to existing originals under the incident
owner's evidence policy; do not print them to perform redaction. Review prior
exports, prompts, logs, and screenshots for exposure without copying secret
values into tracking systems.

### Coordinated rotation procedure

The incident owner coordinates this procedure separately from the diagnostic
fix. Shipping this fix does not rotate credentials or restart workers.

**Blocked prerequisite:** implement and review a non-destructive credential
migration path before changing credentials. Existing targeted/fleet rollout is
not that path: replacement can clear replica prefixes and replace volumes.
Do not use existing rollout while assuming it preserves evidence or volume
identity. The migration must prove that it updates worker environments without
clearing replica objects, replacing/destroying volumes, or losing run evidence,
and must provide a reviewed rollback procedure. Track that lifecycle work
separately from this diagnostic fix. Steps involving worker changes below are
conditional on this prerequisite being met.

1. Inventory affected storage access keys and reporting-token consumers by
   credential identifier only. Record worker, run, image, and volume identities
   and preserve redacted failure evidence before any replacement.
2. Create replacement storage credentials with the required replica/profile
   permissions through the approved secret-management channel. Keep the old
   storage credentials available during the migration until replacement
   replication and restore have been verified.
3. Prepare new storage credentials and a fresh reporting token for the control
   plane and worker configuration without putting values in command
   history or diagnostic output. Plan the reviewed credential migration: the
   reporting endpoint accepts one token, with no overlap/grace mechanism, so
   switching the control token temporarily rejects old-worker telemetry.
4. Once the prerequisite is met, use only the reviewed migration path to update
   control secrets and worker environments. Do not substitute targeted/fleet
   rollout. A simple machine restart retains its old environment. Coordinate legacy-fleet coexistence and
   prevent expected temporary telemetry loss from triggering unintended fleet
   actions. Do not clear replica prefixes, destroy volumes, or overwrite retained
   run evidence.
5. Confirm each migrated worker's machine/run/build and volume identity, accepted
   heartbeats, replication, restore verification, and profile upload where
   applicable. Inspect deployed diagnostic responses through a secret-safe
   assertion that reports only pass/fail and safe metadata; never print raw
   machine/configuration or incident objects.
6. Revoke old storage credentials after all consumers have migrated, confirm
   the old reporting token is rejected, and record revocation identifiers and
   verification results without values.

If validation fails, pause further migration and retain the diagnostic fix,
volume mappings, and evidence. Keep both storage keys valid until the incident
owner selects containment or rollback. An approved storage rollback restores the
previous secret reference only while that key remains valid through the reviewed
non-destructive migration path, then rechecks replication and restore. An
approved reporting token rollback must update the control token and the
environments of already-migrated workers through that same path; switching only
the control plane strands those
workers. Confirm accepted heartbeats for both groups before resuming the rollout.
Reusing an exposed credential extends the exposure and requires the incident
owner's explicit decision. Once old credentials are revoked, issue fresh
credentials and roll forward instead of attempting to reactivate them. Never
roll back to credential-bearing diagnostic output or delete failure evidence.

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

## Upstream PR Retirement

The control plane polls GitHub for every `pr-N` source with a live worker and
retires the fleet when upstream PR `N` is definitively merged or closed. The
signed GitHub webhook at `/webhooks/github` remains available for faster
retirement when the upstream repository sends it. Both paths archive the latest
deployment and worker evidence before destroying Machines, volumes, and S3
replica prefixes. Failures in the soak fleet do not block terminal cleanup.

Polling fails closed. HTTP errors, rate limits, timeouts, empty or malformed
responses, unexpected response shapes, identity mismatches, and unknown PRs
leave the fleet unchanged. The default 15-minute interval makes four requests
per live PR source per hour: five live sources use 20 of GitHub's 60
unauthenticated requests per hour, ten use 40, and fifteen reach the ceiling.
More than fifteen live sources exceed the ceiling, and headroom for other
unauthenticated requests shrinks as the source count approaches fifteen; extend
the interval or explicitly add authenticated API access before operating at
that scale. Polling stops the current cycle on a rate-limit response and does
not retry until the next interval. A rate limit is recorded in the control-plane
event feed immediately and escalated as a sustained event after one hour.

Polling is disabled unless exactly one repository is configured because `pr-N`
source names do not identify their repository. Only repositories in the
allowlist can trigger retirement:

```bash
SOAK_PR_REPO_ALLOWLIST=benbjohnson/litestream
SOAK_PR_KEEP_ALIVE_LABEL=soak:keep-alive
SOAK_PR_STATE_POLL_ENABLED=true
SOAK_PR_STATE_POLL_INTERVAL=15m
SOAK_PR_STATE_RATE_LIMIT_VISIBILITY_THRESHOLD=1h
```

Apply the keep-alive label before closing a long-lived fixture PR when its soak
fleet must remain active. Label comparison is case-insensitive. Removing the
label from an already-closed PR makes the fleet eligible for retirement on the
next successful poll.

To use webhook acceleration, configure the upstream repository to send
`pull_request` events and use the same `GITHUB_WEBHOOK_SECRET` configured on
`soakctl`.

Terminal PR archives use type `teardown` with reason
`upstream_pr_merged` or `upstream_pr_closed`.

## Operator Source Teardown

Retire a failed or otherwise obsolete source fleet with the authenticated
archive-and-destroy endpoint:

```bash
curl -X POST -sS \
  -H "Authorization: Bearer $SOAK_ADMIN_BEARER_TOKEN" \
  "https://litestream-soak-ctl.fly.dev/api/admin/teardown-source?source=pr-1228" | jq .
```

The request requires an explicit `source` and remains synchronous while the
control plane archives the latest deployment and worker evidence before
deleting any resources. It has a 30-minute operation budget, with one
additional minute reserved for writing the response, so keep the client
connection open until the structured result returns.

The result contains an outcome for every worker after the endpoint attempts to
destroy each non-stopped worker's Machine, volume, and S3 replica prefix.
Workers are marked `stopped` only after all three cleanup operations succeed.
If the request is interrupted or reports failed outcomes, calling the endpoint
again is safe: the existing archive is reused, stopped workers are skipped,
and incomplete workers remain retryable.

Operator teardown archives use type `teardown`:

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/run-archives?source=pr-1228&type=teardown" | jq .
```

The `main` fleet has an additional safeguard and is rejected unless the
request includes `confirm_main=true`.

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

That resumes dormant workers using the latest ready deployment recorded for the
requested source. If no ready deployment exists for that source, the request
returns `409` without creating machines. To select a specific ready deployment,
add both `&sha=<soak-git-sha>` and
`&litestream_sha=<upstream-litestream-sha>` to the request.

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
`Dockerfile.worker`. The workflow variable is optional because the build
resolves its default of upstream `main` to a concrete commit; the resolved SHA
is still required in every deployment-ready notification.

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

The image ref and upstream Litestream SHA are required. The script rejects the
notification locally when either value is missing instead of sending an
incomplete deployment to the control plane.

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

If you only need to wake dormant workers with the latest ready deployment
recorded for `main`:

```bash
curl -X POST -sS \
  -H "Authorization: Bearer $SOAK_ADMIN_BEARER_TOKEN" \
  "https://litestream-soak-ctl.fly.dev/api/admin/resume-dormant?source=main&trigger=manual_resume" | jq .
```

## Fly Health Checks

Both Fly apps declare `/healthz` checks in their Fly config.

- `fly.control.toml` uses `[[http_service.checks]]` on the control-plane HTTP
  service. The endpoint is unauthenticated for `GET` and `HEAD`, returns `200`,
  and should stay lightweight.
- `fly.toml` uses top-level `[checks.healthz]` on port `9091`, the same private
  port used by worker metrics. Do not add an `[http_service]` block to the
  worker app just for health checks; that would expose the worker metrics and
  health port publicly.

The worker check uses a `5m` grace period so deploy health checks do not race
the Litestream restore and first-sync path. Before changing either config,
validate both files locally:

```bash
fly config validate -c fly.toml
fly config validate -c fly.control.toml
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
The notification records `repo_full_name` with the deployment. The control
plane creates or updates a PR-specific worker fleet under that source
automatically and refuses poll-based retirement when the recorded repository
is missing or differs from the polling configuration.

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
- notifies the control plane with `source=pr-1221` and its repository identity

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
- `platform_disk_full_no_progress`: the constrained-disk scenario saw no replica progress under confirmed disk pressure before `litestream_disk_full` appeared with the distinct staging log
- `platform_disk_full_recovered`: `litestream_disk_full` appeared with the distinct staging log, the harness freed reserved disk space immediately, the gauge cleared, and Litestream replicated again without restart
- `platform_disk_full_recovery_failed`: `litestream_disk_full` appeared with the distinct staging log, but Litestream did not replicate again after reserved disk space was freed
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

### Pulling pprof captures from a worker

Every worker begins capture before waiting for initial sync, waits up to five
seconds for the control socket, and takes baseline heap, allocs, goroutine,
and text MemStats evidence plus a five-second startup CPU sample taken first.
Hourly capture continues. Verification failures, sync degradation/recovery,
and disk/metrics condition changes queue incident captures, including
recovered conditions; existing failure reports remain independent. Shutdown has a ten-second total budget: cancel and join collection, allow up
to five seconds for final non-CPU capture while Litestream remains running,
then use a three-second upload batch bounded by the remaining deadline. Final
artifacts are uploaded newest first, ahead of older pending captures. A
crashed process may only produce unavailable manifests.

Capture sets are serialized and bounded to 45 seconds. CPU sampling is limited
to once per minute; rate limiting, cancellations, a full 16-event queue, and
storage exhaustion are recorded in `status.json` (cumulative reason counts and
the latest 64 events). Each artifact is limited to 8 MiB. Local retention
keeps 96 non-baseline artifacts and four baseline artifacts with their JSON
manifests. Pending S3 uploads are excluded from pruning. At 256 directory
entries, new capture stops with an explicit unavailable log until evidence is
retrieved and space is freed. Uploads and retry batches each have a
three-second deadline; pending uploads are retried by a separate loop after
capture and every 30 seconds, including after restart. There is no automatic
expiry of failed uploads. This bounds disk use without deleting unuploaded
evidence. Capture itself is best effort; logs report failures and
queue/storage limits rather than treating missing evidence as success.

A delivered remote manifest reports `upload=uploaded`; the local manifest
stays pending until both artifact (when available) and manifest delivery
succeed. A failed attempt stores `upload_error` separately from capture errors,
with stderr limited to 4 KiB and configured credentials redacted. Successful
artifact and manifest delivery clears that current error. The first failure
and latest 15 failures remain in `upload_failures`, with UTC timestamps, attempt numbers, delivery
stage, and sanitized causes. `upload_attempts` counts all attempts and
`upload_failure_count` counts failures.
When capacity is exhausted, the first cause remains, `upload_failures_dropped`
counts omitted entries, and `upload_history_incomplete=true` explicitly marks
the partial history. Legacy pending manifests without attempt counters are
also marked incomplete: their earlier causes and counts were not recorded.
Counters cover attempts observed by this uploader. Delivered manifests retain
this history after recovery. An empty `upload_error` describes current delivery
health only; it does not clear `upload_failure_count` or make incomplete legacy
history clean. Read-only incident snapshots should retain these fields together
with capture availability and run identity rather than reduce them to one pass
flag. A manifest without earlier attempt counters cannot establish zero prior
failures, even after successful delivery.

The merged uploader and its local signed-request CI test do not establish live
Fly-to-Tigris delivery. After deployment, independently retrieve the remote
artifact and manifest, compare their identity and artifact digest with local
evidence, and retain earlier delivery failures. Until that check is performed,
report live remote delivery as unverified.

On September 9, 2026, the coordinator verified delivery on deployed harness
`8346a06905b3ea0f7134cb6c9507fc7ca920c9c4`, worker `worker-main-gharchive`,
machine `e820910cd406d8`, run `d753b920-dd7a-45c0-ae1f-37b9f42a548a`.
Remote GETs for startup CPU, baseline heap, allocs, goroutine and MemStats
artifacts plus all five JSON manifests exited successfully and matched local
bytes. The retained audit proof records SHA-256 for all ten objects.
The goroutine manifest's first delivery attempt failed at
`2026-09-09T17:55:32.022789126Z` with `signal: killed`; attempt two delivered it.
Both local and remote manifests retained `upload_failure_count=1` and the
original manifest-stage failure. This is verified delivery with a recovered
incident, not a historically clean run or proof for every worker. Run reliability
must retain this failure independently of the current uploaded status.

Uploads use the configured credentials, ignoring inherited AWS credential overrides and user s3cmd config.
The process deadline bounds CLI retries; `--max-retries` is not a supported
s3cmd option. CI exercises the installed CLI with signed local S3 requests.
Failed final uploads remain local and cannot survive destruction of
the volume. Retain and retrieve the volume when shutdown reports upload
failure.

JSON sidecars include shared run/deployment/workload identities, independent
candidate/workload/worker SHA, effective workload hash/configuration,
validator ID/version, phase, image, capture availability, upload state, and
installed binary SHA-256 and Go build/runtime metadata. Missing binaries/build
identity are explicit. Use the recorded image and binary digest to retrieve
matching binaries; never assume the current branch matches an older profile.
Uploads follow the replica endpoint's HTTP/HTTPS scheme and force-path-style
addressing. Credentials are passed through environment variables, not command
arguments or logs.

Set `SOAK_PPROF_CAPTURE=false` to disable capture. `SOAK_PPROF_BLOCK=true`,
`SOAK_PPROF_MUTEX=true`, and `SOAK_PPROF_TRACE=true` opt into additional
endpoint requests (trace is one second). Unsupported endpoints produce
unavailable manifests. Block/mutex sampling must already be enabled by the
target binary; an available endpoint does not prove sampling is enabled; its
manifest labels sampling as unverified. The collector never changes the
target's runtime sampling configuration. No fleet scenarios are enabled by
these changes.

Set `SOAK_PROFILE_MATCH` to a timestamp or phase substring to select matching
artifacts. The mutable `status.json` is refreshed on retrieval.

`scripts/pull-profiles.sh <source> <profile> [count]` downloads the newest
binary profiles, text evidence, and matching JSON manifests straight off the machine with flyctl (`<profile>` is the worker-name
suffix such as `high-vol` or `many-dbs-100-dir`; the profile name such as
`high-volume` also works when `SOAK_BASIC_AUTH_USERNAME`/`PASSWORD` are set,
since the script then resolves it through the control plane) (it reads the access token from
`~/.fly/config.yml` when `FLY_ACCESS_TOKEN` is not exported) into
`tmp/profiles/<source>/<profile>/` and prints the `go tool pprof` commands,
including a `-diff_base` comparison against `main` when that profile has been
pulled for both sources:

```bash
scripts/pull-profiles.sh main many-dbs-100-dir
scripts/pull-profiles.sh pr-1483 many-dbs-100-dir
go tool pprof -top -sample_index=inuse_space -diff_base=tmp/profiles/main/many-dbs-100-dir/<heap>.pprof tmp/profiles/pr-1483/many-dbs-100-dir/<heap>.pprof
```

Fleet databases are small, so page-index terms from very large databases will
not show up here; use the `snapshot-compaction-overlap` local rig for those.
This is for spotting new allocation sites, goroutine growth, and CPU hot spots
between `main` and a PR fleet.


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

## Many-DB Scaling Tiers

The many-DB tier ladder is flag-gated in `internal/orchestrator/fleet.go`:

- `SOAK_ENABLE_MANY_DB_FLEET=true` enables `many-dbs-100-list` and
  `many-dbs-100-dir` (2 load workers, 10 GB volume, 2048 MB, 1 CPU each).
- `SOAK_ENABLE_MANY_DB_500=true` (nested, inert without the base flag) adds
  `many-dbs-500-list`, `many-dbs-500-dir`, and `many-dbs-500-dir-lowfreq`
  (3 load workers, 15 GB volume, 3072 MB, 2 CPUs each — three machines).
- `SOAK_ENABLE_MANY_DB_1000=true` (nested) adds `many-dbs-1000-dir`
  (4 load workers, 20 GB volume, 4096 MB, 2 CPUs).

`many-dbs-500-dir-lowfreq` is the reduced-frequency control for
`many-dbs-500-dir`: same workload with snapshot 1h, L1/L2/L3 compaction
5m/30m/6h, L0 retention 1h checked every 2m. The default profiles omit those
keys so upstream Litestream defaults apply.

Many-DB baseline profiles connect Litestream directly to Tigris. Keep
`SOAK_ENABLE_S3_OBSERVE_PROXY` unset: `observe` mode remains disabled and
unfixed because the proxy forwards Litestream's local proxy `Host`, which
Tigris interprets as the bucket name.

The S3 proxy is supported only for profiles that deliberately inject faults.
In those modes it replaces stale SigV4 signing headers, rewrites the request
for the target host, and signs it again with the worker's configured S3
credentials and region. This makes the proxy a traffic-mutating transformer,
not a passive observer. Never use it for baseline soak measurement: those
results would measure Litestream talking to the transformer instead of
Litestream talking directly to Tigris.

The soaked Litestream version does not count `ListObjectsV2` operations on its
metrics endpoint. A passive LIST counter therefore requires an upstream
Litestream change, which is outside the scope of the fault proxy. Consequently,
baseline many-DB profiles do not populate
`soak_control_worker_s3_list_requests_total`.

Related per-worker series on the control plane:

- `soak_control_worker_s3_list_requests_total` — monotonic LIST count; a
  gauge populated only while an explicit fault-injection profile uses the
  proxy, so it resets to 0 on worker restart. Use
  `delta(soak_control_worker_s3_list_requests_total[1h])` for LIST/hour and
  treat negative deltas as restarts.
- `soak_control_worker_litestream_heap_inuse_bytes`,
  `soak_control_worker_litestream_stack_inuse_bytes`,
  `soak_control_worker_litestream_alloc_bytes_total`, and
  `soak_control_worker_litestream_alloc_rate_bytes_per_second` — Litestream
  process memory, sampled from its `/metrics` endpoint every poll.

For the tier comparison table, pull idle CPU
(`rate(soak_control_worker_litestream_cpu_seconds_total[1h])`), goroutines,
heap/stack in-use and allocation rate per profile. Dividing aggregate values by
database count gives an average, not per-database attribution. LIST/hour is
unavailable for direct-to-Tigris baselines; do not substitute zero. Compare `many-dbs-500-dir` against
`many-dbs-500-dir-lowfreq` for the reduced-frequency lever.

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


## Scenario inventory

This inventory describes configured behavior, not executed results. Default fleet
reconciliation activates the 13 profiles below for `main` and supported `pr-N`
sources. Explicit worker environment overrides can differ from fleet defaults;
retain the effective configuration and hash with every result. A profile name or
UI charter expresses intent and does not prove throughput, contention, fault
engagement, or a clean historical run.

Every default row uses the worker restore pipeline: quiesce load, sync, restore,
validate integrity and the independent logical source snapshot. Record individual
failed, aborted, pending, and passed cycles. A pass covers that snapshot only.
All require compatible Litestream and independent workload binaries, SQLite,
a usable replica, and storage. Fleet runs additionally require Fly and Tigris;
local file-replica results do not establish provider behavior. Runtime, process,
and profile measurements require the corresponding endpoints and OS support;
missing/stale observations remain unavailable. The row-specific requirements and
limits below supplement this shared contract.

| Default profile | Configured trigger/work and actual evidence to inspect | Assertions and limitations beyond the shared restore check | Additional capabilities |
| --- | --- | --- | --- |
| `low-volume` | Constant synthetic target 10, one generator worker, 1 KiB payload; inspect completed work and errors. | Baseline restore correctness; low offered load does not isolate every platform or harness failure. | Synthetic generator. |
| `high-volume` | Wave target 500, eight workers, 4 KiB payload; inspect actual writes, lag, WAL and resource samples. | No automatic proof of saturation or sustained 500 writes/s; 100 GB volume is capacity, not database size. | Synthetic wave load; multipart configuration. |
| `burst-volume` | Burst target 1000, four workers, 2 KiB payload; inspect burst work and subsequent lag/WAL drain. | Sampled maxima can miss spikes; configured bursts do not prove a backlog formed or drained. | Synthetic burst load. |
| `read-heavy` | Target 80, six workers, read ratio 0.95; inspect generator work/errors and restore results. | Read ratio is configuration; no application read-latency or stale-read oracle is implied. | Generator read support. |
| `gharchive-replay` | Loop the configured GH Archive hour at speed 300; inspect attempted events, affected rows, no-ops, errors and schedule lag. | Insert replay is not a GitHub service workload; speed is timestamp scaling, not achieved throughput. | Download/decompress the configured archive. |
| `gharchive-mixed` | Archive speed 120 plus wave target 50 with two synthetic workers; inspect both producers independently. | One producer succeeding cannot establish work by the other. | Archive and synthetic capabilities. |
| `taxi-replay` | Loop bundled taxi CSV at speed 90; inspect committed replay work and lag. | Small fixture insert coverage, not a production taxi database or query benchmark. | Bundled taxi dataset. |
| `taxi-mixed` | Taxi speed 60 plus wave target 40 with two synthetic workers; inspect both producers. | Concurrent timing and commit order are not reproducible from speed alone. | Taxi and synthetic capabilities. |
| `orders-replay` | Loop bundled orders JSONL at speed 45; inspect committed events/errors. | Order-shaped inserts do not implement checkout, payments, or a full transactional application. | Bundled orders dataset. |
| `low-vol-syd` | Low-volume configuration in Sydney; inspect actual lag and restore results. | Regional diagnostic; legacy-score exclusion does not exclude retained reliability. Distance alone is not measured network latency. | Fly Sydney placement. |
| `high-vol-ams` | High-volume configuration in Amsterdam; inspect actual load, lag and resources. | Regional diagnostic; included in retained reliability; no cross-region throughput guarantee. | Fly Amsterdam placement. |
| `overload-truncate0` | Constant target 600, eight workers, 2 KiB payload, zero truncate threshold; inspect WAL/backlog and completed work. | Restore check does not assert WAL boundedness or prove overload engaged. | Candidate accepts `truncate-page-n: 0`; adequate disk. |
| `pinned-reader` | Constant target 200, four workers; companion transaction holds 4m with 45s pauses; inspect reader events and WAL/checkpoint observations. | Require actual reader engagement before attributing checkpoint effects; no query-latency guarantee. | Companion SQLite reader. |

Many-database profiles are opt-in and excluded from the legacy release score;
they participate in retained complete-run reliability. All seed a
fixed set, rotate a 2% active subset, and use an in-process writer. Inspect
committed work, pending database count/age, individual restore outcomes, and
aggregate process/runtime samples. A successful batch is not full coverage;
untouched and deferred databases remain obligations. These profiles do not
exercise dynamic tenant creation/removal. Directory mode requires candidate
directory discovery support; list mode requires explicit database configuration.
Neither mode supplies native passive S3 LIST counts.

| Opt-in profile | Activation | Actual workload and assertion | Specific limitation/capability |
| --- | --- | --- | --- |
| `many-dbs-100-list` | `SOAK_ENABLE_MANY_DB_FLEET=true` | 100 configured databases, two writer workers; rotating restore coverage. | Explicit list support; aggregate metrics cannot locate a per-DB leak. |
| `many-dbs-100-dir` | Same base flag | 100 directory-discovered databases, two writer workers; rotating restore coverage. | Directory support; verify discovery rather than infer it from configuration. |
| `many-dbs-500-list` | Base flag plus `SOAK_ENABLE_MANY_DB_500=true` | 500 listed databases, three writer workers; rotating restore coverage. | List support; incomplete batches remain pending. |
| `many-dbs-500-dir` | Base flag plus 500 flag | 500 directory databases, three writer workers; default maintenance cadence. | Directory support; sampled resources are not exact peaks. |
| `many-dbs-500-dir-lowfreq` | Base flag plus 500 flag | Same 500-directory workload with relaxed maintenance intervals. | Candidate interval support; compare matched completed work and age, not just profile averages. |
| `many-dbs-1000-dir` | Base flag plus `SOAK_ENABLE_MANY_DB_1000=true` | 1000 directory databases, four writer workers; rotating restore coverage. | Directory support and sufficient resources; not a demonstrated maximum supported DB count. |

Queue and cache are explicit `LOAD_MODE=queue` and `LOAD_MODE=cache` workloads,
not default fleet entries. Both require the churn-capable worker, a dedicated
SQLite database and compatible replica/restore binaries. See the
[churn contract](churn/README.md) for complete configurations and execution.
Queue measures affected rows and transaction attempts across enqueue, claim,
retry, completion/receipt, expiration and deletion; its restored-state assertions
include valid transitions and receipt consistency. Cache measures upserts and
expiration sweeps and checks bounded keys and valid values/expiry ticks.
Both use the shared logical oracle, retain busy/other failures, and report
attempt latency excluding pacing. Logical TTL is not wall-clock expiry;
conditional no-ops are not mutations, and concurrent commit order is not fixed
by the seed. Bounded live rows do not bound physical disk or WAL size.

### FTS maintenance

`PROFILE=fts-maintenance` or `LOAD_MODE=fts` explicitly activates the worker's
built-in FTS5 workload; it is absent from the default fleet. The versioned
64-document corpus repeats eight committed phases: insert, update, delete,
query, merge, update, query and optimize. `WRITE_RATE` targets phases per second,
not rows; synthetic payload/read/worker settings do not alter the corpus.
Committed progress resumes after restart. FTS cannot be combined with many-DB.

Inspect committed operation counts separately from affected document rows,
checked queries and actual maintenance changes. Match counts, phase duration and
failures remain separate measurements. Verification pauses at a committed
boundary and compares document rowids/values, visible FTS columns and recognized
FTS5 shadow objects with the shared logical oracle. Independent document-table
search checks cover terms, phrases, prefixes and absent tokens on both source
and restore. Unknown virtual modules remain unsupported; shadow objects are
compared rather than omitted. An independently rebuilt equivalent index can differ
from the exact replicated state this oracle requires.

Required capabilities are SQLite FTS5 in the worker and restore validator,
compatible pinned replication/restore binaries and the FTS-aware logical oracle.
Initialization or phase failure is retained and stops the workload. See the
[FTS restore comparison](../README.md#opt-in-fts-restore-comparison) for immutable
binary inputs and the explicit opt-in real test; absent binaries mean skipped.

Litestream profiles bracket phase boundaries; the separate `worker_cpu` capture
covers SQLite phase execution. These measure different processes and intervals.
The coordinator-reviewed same-binary development calibration completed 16 restore
boundaries and parsed 82 profile artifacts, but all 16 worker CPU profiles had
zero samples. Parsing success therefore supplies no CPU-performance evidence.
Earlier failed attempts remain retained. This calibration is not a demonstrated
main-versus-candidate improvement or proof of remote profile delivery. Short
phases, profiler conflicts, unavailable endpoints and upload failures must remain
explicit in comparison evidence.

### Deliberate fault rigs

Invoke `bash scripts/local-rig-one-shot.sh <scenario> <immutable-sha> <runs>`
only against disposable fixtures. These are opt-in local scenarios, not fleet
activation requests. The runner builds the selected upstream source and requires
Git, network/module access, Go and a C compiler. S3 scenarios use local MinIO via
Docker Compose; constrained disk requires the runner's constrained filesystem.
Record resolved SHA, compiler, fixture parameters, full logs, structured outcome,
fault engagement, and every attempt. Local MinIO behavior does not establish
Tigris behavior. Missing capabilities or unengaged fixtures are not passes.

| Scenario/profile | Trigger and actual measured work | Oracle/assertions and limitations | Specific capability |
| --- | --- | --- | --- |
| `compaction-source-stream-drop` | Drop source GET streams during compaction; count GET/range GET exposure and inspect L2 coverage. | Expected error signature or resumed compaction depends on the tested branch; a boolean alone is not recovery proof. | S3 fault proxy and compactor API/range reads. |
| `uploadpart-retry-quota` | Reset multipart requests; count injected failures and unique parts. | Checks retry-quota signature or expected fault/part counts; not full application restore equivalence. | Multipart upload and reset-capable proxy. |
| `provider-http-408` | Inject one provider HTTP 408 during restore; retain request count and restore output. | Requires injection and restored fixture rows; not a general provider retry guarantee. | Compatible S3 restore and proxy. |
| `provider-request-canceled` | Inject HTTP 408 with `RequestCanceled` on listing. | Requires one injected failure and restored fixture rows; timeout extensions are not SDK retry fixes. | S3 listing and proxy error-body support. |
| `constrained-disk` | Fill constrained fixture; inspect disk-full signal and TXID progress before/after recovery. | Distinguish expected failure detection from recovered progress; do not infer recovery from process survival. | Actual constrained filesystem and disk-full metrics/logs. |
| `l0-gap-heal` | Remove an interior fixture L0 above L1; inspect gap detection/re-upload and restored rows. | Requires engaged gap and complete fixture restore; opt-in destructive fixture operation, not a production repair procedure. | Isolated S3 prefix and retained local L0. |
| `snapshot-compaction-overlap` | Gate snapshot stream during L1 work; compare sampled heap growth with sequential phases. | Enforces fixture memory budget and retains gate reason; growth/final heap captures are not exact peak profiles. | Upstream maintenance APIs, sufficient local/object storage; APFS cloning optimization on macOS. |
| `restore-retention-race` | Concurrent writes, restores, compaction and retention; retain actual L0 opens/deletions during restore. | Requires exposed, logically valid restore; earlier failures block clean success. Sampled plans are not actual complete restore plans. | Maintenance logs, object-open observations and fixture prefix oracle. |

Worker-only aliases `s3-flap` select the uploadpart fault configuration and
`provider-408-requestcanceled` selects `provider-http-408`; the latter name must
not be mistaken for the separate `provider-request-canceled` error-body fixture.
Selecting a worker profile with `PROFILE` does not invoke the one-shot rig or
establish its assertions. Keep the S3 observe proxy disabled for baseline runs.
Calibration remains separate: retain repeated known-bad/base and known-fixed/head
runs, including the #107 requirement of base 3/3 failures and head 3/3 passes.
No successful unit test establishes that separation or authorizes Fly A/B.

### Local lifecycle, schema and backlog scenarios

These merged runners remain opt-in and do not add fleet workers. Their detailed
guides contain invocation parameters, retained artifact formats and separate
executed-development evidence. Require pinned binaries and a new isolated output
directory; retain every failed attempt when repeating a scenario.

| Scenario and trigger | Actual work and metrics | Oracle/assertions | Capabilities and limitations |
| --- | --- | --- | --- |
| [Fresh start](persistent-upgrades.md), `soakupgrade -mode fresh-start` | Each arm starts with its own binary and independent new state; fixed-count deterministic churn and pre/post restores. Retains binary/compiler identities and every check. | Integrity plus shared logical schema, application metadata and typed rows; supported rollback is separately checked. | Explicit binary digests and `ltx-v1` transition contract; local file replica only. Same-binary calibration is not version separation. |
| [Persistent upgrade](persistent-upgrades.md), `soakupgrade -mode persistent-upgrade` | Age baseline through at least two snapshots, two compactions, positive retention deletion and committed updates/deletes; seal quiescent complete state and copy independently into both arms. | Validate before/after continuation and declared rollback against candidate-written history. Fixture incidents survive reuse; missing aging exposure is inconclusive. | Compatible pinned formats and declared rollback support; no provider object-copy/version-history or fleet migration evidence. Elapsed age alone is insufficient. |
| [Schema and reclamation](schema-fixture.md), `schemafixture` with `-vacuum vacuum` or `incremental` | Growth, deletion, reuse, indexes, column/backfill, table rebuild, rollback and reclamation across ten paused boundaries. Measures operation/verification duration, pages/freelist, local bytes/headroom and S3 object bytes when selected. | TXID-pinned restore, shared logical oracle and application row/payload checks. Incidents prevent clean success. Low-headroom variant must engage actual constrained storage. | Compatible sync/TXID restore; optional S3/emulator credentials and isolated prefix. Boundary samples are not peaks or atomic bucket snapshots. Disk-full negative-control command success still describes a failed scenario. |
| [Offline backlog](offline-backlog-recovery.md), one-shot `offline-backlog` | Idle, injected 503 outage, latency, 429 throttling and reconnect while writing; records request/operation journals, row lag, net drain, sampled storage/RSS and observed container limits. | Requires faults, optional pinned reader, backlog and drain under continuing writes, plus final prefix validation. Injected/provider failures, retries and warnings remain incidents after recovery. | S3 proxy and explicit emulator/provider classification; cgroups/mount evidence for actual limits. Effective writer concurrency is one. Final recovery after writers stop does not establish drain under load. |

### Crash, historical restore and tenant lifecycle

[The recovery runner](recovery-rig.md), `soakrecovery`, activates the following
bounded local file-replica controls against an explicitly hashed binary and new
fixture directory. Every attempt retains duration, errors, logs and exposure.
The common oracle validates integrity, typed append-only transaction prefixes,
and shared schema/application metadata. Schema remains constant in this fixture;
historical rows are not required to equal the latest source rows.

| Recovery control | Actual trigger/work and assertions | Capability and measurement limits |
| --- | --- | --- |
| Replicator crash/restart | Confirm child SIGKILL, commit while offline, validate available replica, restart and restore again. | Process interruption is not power loss. Report committed, restore-confirmed and unconfirmed row boundaries separately; row loss is not time-based RPO. |
| Interrupted restore | Kill only after nonempty restore output, then validate recovery to a separate destination. | A restore finishing before the kill is unengaged; process termination without valid recovery is not success. |
| Follow resume | Interrupt follow, reopen the same output and saved TXID sidecar, observe resume and validate a new commit. | Requires advertised follow flags and the selected pin's resume contract; unsupported checks remain unexecuted. |
| Restore during maintenance | Restore while writes continue; require actual L0 opens, compaction and positive retention deletion during the restore process. | Subsequent oracle time does not count as exposure. Individual unexposed attempts remain observations; at least one valid exposed restore is required. |
| Timestamp/retention | Restore a target before later commits, repeat after retention, and validate latest recovery separately. | Requires advertised timestamp support. Retained targets match their original prefix; expired targets require the pin's explicit unavailable error. Later excluded commits are not asynchronous loss. |
| Local loss | Quarantine source, sidecars and cache, then recover from the remaining replica and compare with the retained oracle. | Generated fixture state only; no proof of provider durability, actual hardware loss or aged-format upgrade compatibility. |

Earlier unexpected errors, warnings, retries and self-healing messages prevent a
clean recovery verdict. Expected kills and expired-target errors remain visible.
The shared `CompareLogicalSchemas` wrapper supplies schema/application-metadata
comparison without row digests; the recovery prefix oracle still validates rows.
This complements full `CompareLogicalDatabases` equality rather than weakening it.

[The tenant fixture](tenant-lifecycle.md), `tenantfixture`, is separately opt-in
with `-mode static` or `watch` and 2–1000 tenants. It seeds all but one tenant,
restores initial state, creates the last tenant at runtime, writes ten hot-tenant
rows and one per cold tenant, retires the hot generation, recreates it under a
new name/prefix, then restarts and restores live and retained generations. Static
mode requires non-discovery before restart; watch mode requires runtime discovery
and removal. Every tenant restore uses the bounded logical oracle and exact
generation identity. It requires the declared `directory-v1` configuration,
watcher/list/sync/file-restore contract and a pinned candidate.

Inspect per-tenant phase results, pending/attempted identities, oldest pending
age, registry counts and retained process incidents. Linux read-only `/proc`
frames provide RSS, CPU counters and FDs; unsupported platforms report that
explicitly. Removal requires registry disappearance and, on Linux, no open source
or sidecar descriptors. CPU counters reset on restart. Cleanup frames establish
process exit, not a universal leak threshold. Writes/restores are serialized;
this is neither same-filename/prefix reuse nor S3/provider coverage.

A two-tenant smoke does not establish 100/500/1000 scale coverage. Preserve each
tier's failures and partial outcomes independently. Cancellation leaves
unscheduled identities pending; interrupted work is incomplete unless prior
failures already make the run failed. Per-request/operation timeouts during an
active run remain failures. Changing deadlines or retrying a tier creates a new
experiment and cannot replace the original failed evidence. No clean full-scale
claim follows from a later small smoke or successful cleanup.

The shared `worker.CompareLogicalDatabases` entry point exposes the worker's
bounded logical comparison to local rigs: schema, application metadata and typed
row contents under the same recognized bookkeeping policy. It is an equality
oracle, not a workload generator, fault-engagement detector or performance gate.
Fixture-specific application and historical-prefix checks remain separate.

## Evidence required for comparisons

The [merged reliability contract](run-reliability.md) exposes `reliability` and
`reliability_findings` alongside the legacy latest-result scorecard. All regions
and profiles contribute retained evidence. Clean eligibility requires at least
two attributed completed verifications, the configured measured span (24 hours
in comparison reports), and no leading, internal or trailing gap over one hour.
Active runs measure trailing gaps to the present. Real workload progress,
positive-size snapshot and compaction completions, and retention with positive
deletions are all required, along with current pass, complete history/attribution
and no unexpected incident or incomplete observation. Configured intervals and
zero-deletion retention are not maintenance exposure. Short local experiments
cannot earn this long-soak eligibility merely by restoring correct data.

Pending verification adds neither a completed check nor a failure and blocks
current eligibility. Exact event identities deduplicate replay; per-epoch counter
maxima retain progress/errors without inflating them. Recovered profile failures
remain incidents after delivery and manifest pruning. Disabled sampling and CPU
rate limits are neutral, including when they fill the recent status ring; lost
adverse history remains incomplete. Comparison credit additionally requires
matching workloads/profiles, equal completed-check counts and measured spans
within one minute. Missing or ambiguous profile pairs remain inconclusive.

The control plane journals received evidence independently of worker replacement
and raw-history pruning. Its journals have no destructive retention policy, so
monitor disk and query cost. The bounded worker outbox survives restart only on
retained storage; persistence/capacity failures stop work, and destroyed or
unobserved evidence cannot be reconstructed. Quiet logs cannot exclude unlogged
SDK retries. These are observation limits, not permissions to assume a clean run.

Treat current health and historical outcome separately. A later successful
restore can show recovery while the original failure remains an incident.
Recovered, aborted, pending, unsupported, and unexecuted checks must remain
visible. Archive failures before replacing workers; never turn a failed attempt
into a clean pass by retrying it or averaging it with faster runs.

| Comparison | Required setup and evidence | What it cannot establish alone |
| --- | --- | --- |
| Fresh | Independently initialized baseline and candidate fixtures with fixed dataset, seed, offered work, replica settings and resources. | Upgrade compatibility with aged replicas. |
| Persistent | Recorded aged source/replica state, isolated copies, age/churn history and before/after logical validation. Record fresh fallback or failed volume fork. | A fresh fallback cannot earn persistent-upgrade coverage. |
| Recovery | Observed crash/fault/offline engagement, committed and confirmed-replicated boundaries, retained interrupted attempts and validated restored state. | Process restart alone does not prove recovery; final catch-up after stopping writes does not prove drain under continuing writes. |

Pin baseline, release and candidate to immutable SHAs; record independent
workload and validator identity, binary digest/compiler, effective configuration,
run/machine/deployment identity, source age, region, provider and resource limits.
Match completed work as well as offered rate. Keep favorable, representative and
saturated load conditions separate; include no-Litestream controls when measuring
replication overhead, repeat runs and vary order to expose warm-cache/order bias.
Report spread and missing observations rather than a single favorable delta.

Metric collection must be read-only. A sync/checkpoint request changes the
experiment even when it does not wait; retain any such earlier run as an
observer-intervention result. If the selected pin lacks read-only transaction-lag
observations, report lag unavailable rather than force sync or infer zero lag.
S3 transferred upload/download bytes and retained file-replica bytes describe
different quantities and must not share a comparable storage-size label.

Assess correctness, incidents, recovery, latency, throughput, lag, CPU, memory,
allocation, disk/WAL/backlog, file descriptors and provider requests separately.
An improvement in one dimension never cancels a regression in another. Unknown
capability is not zero cost. Compare CPU intervals and allocation rates with
matched durations/work; distinguish RSS, live heap, allocation and sampled
maxima. Per-process totals divided by database count are averages. Pprof captures
require matching binaries and phase; availability, parse validity and successful
remote delivery are separate checks. Block/mutex endpoints do not prove sampling
was enabled. Profile collection itself can perturb resource measurements.

### Controlled local comparison runner

Use [soakcompare](reproducible-comparisons.md) for explicit paired local
experiments; deployment scorecards remain observational. Pin main, baseline,
release or candidate refs once, record the clean harness/compiler and binary
digests, inspect an immutable fixture, and execute the pinned plan with a new
experiment ID. The runner performs `16 × repeats` executions: main/main
calibration and baseline/candidate comparison, both arms, and four controls.
Pair order alternates; each arm gets isolated fresh state and the same operation
budget. It never activates a fleet.

| Comparison control | Actual work | Assertions and limits |
| --- | --- | --- |
| Favorable | One committed 128-byte insert per operation, with 1ms pacing. | Shared logical plus independent expected-operation/fixture validation; favorable is a named control, not a universal best case. |
| Representative | One committed 1,024-byte insert per operation, with 1ms pacing. | Same correctness checks; does not represent every production workload. |
| Saturation pressure | Unpaced single-writer 16,384-byte inserts. | Same correctness checks; saturation must be observed rather than inferred from the name. |
| No Litestream | Paced 1,024-byte inserts without a replica process. | Validate source rows; candidate allocation, FD, lag, restore and object-request metrics are unavailable. |

File or S3 replication, bounded final sync, shutdown and restore run against the
selected real binary. Compatible CLI/configuration and required endpoints must
be observed; unsupported execution is retained as failure. Linux FD observations
require `/proc`; allocation requires candidate pprof TotalAlloc, and transaction
lag requires read-only source/replica TXIDs. The exercised 4ed7 pin exposes local
TXID and last-sync time but no replica TXID, so transaction lag is unavailable;
replica sync age is a separate metric. S3 request and transfer counts include
observer overhead, retries and restores; retained S3 size is unavailable without
inventory. The detailed guide defines CPU, RSS, allocation, latency and disk
measurement scopes; none is interchangeable with another.

Reports retain paired deltas, Student-t 95% intervals and a main/main noise floor.
Missing pairs/capabilities and noisy intervals prevent directional performance
claims. Host contention, temporal dependence and multiple comparisons limit
inference. `no_adverse_observed` means no adverse evidence in completed scheduled
runs, not a clean-soak pass. A single final verification has zero measured span;
local artifacts do not establish continuous history, fleet eligibility or durable
fleet-journal delivery. Preserve all failures and partial executions. Earlier
collector tests with placeholder identities or synthetic fixture age are not
clean provenance/aging evidence; mutating sync-probe runs remain separately
labeled observer-intervention evidence.

### Unresolved measured incident

[Issue #255](https://github.com/corylanou/litestream-soak/issues/255) tracks a
retained level-9 snapshot failure in the clean comparison CLI campaign: all 32
executions restored correct data, but one baseline saturation execution logged
`write snapshot ltx: read database page 306: invalid argument`. The campaign's
overall verdict is `adverse` and performance is `inconclusive`.
Both arms used Litestream `4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3`; the
clean Go 1.25.13 harness was `211975ccffcd5512d64c2df5307df750f77454da` on
macOS arm64 with a local file replica. This same-binary run demonstrates neither
candidate improvement nor a root cause or fix. A similar error during tenant
calibration is a related observation, not proof of a common cause. Preserve the
original report and per-execution artifacts; later correct restores or quiet
repeats cannot replace the failed evidence. Investigation remains separate from
these operating procedures.

[Issue #257](https://github.com/corylanou/litestream-soak/issues/257) separately
tracks control-plane startup and scorecard delays observed after the reliability
deployment. A rollout request completed in 0.51 seconds while comparison requests
timed out at 20 and 30 seconds; an intervening comparison took 5.08 seconds.
These are different endpoints and retained attempts, not a clean latency result.
The exact contribution of startup migration, I/O, decoding and contention remains
unestablished. Follow-up harness remediation and live validation are separate
from the Litestream snapshot investigation in #255.

## Validation and deployment evidence checklist

For a local change, run these from the checkout and retain exit status/output:

```sh
GOTOOLCHAIN=go1.25.13 go build ./...
GOTOOLCHAIN=go1.25.13 go test ./...
GOTOOLCHAIN=go1.25.13 go test -race ./...
GOTOOLCHAIN=go1.25.13 golangci-lint run
GOTOOLCHAIN=go1.25.13 govulncheck ./...
bash scripts/test-compatibility.sh
```

Ordinary tests can skip real-binary scenarios when required binaries are absent;
record that as unexecuted. Changed-source coverage measures changed source files;
documentation-only diffs have no changed-source percentage. The compatibility
runner retains identities and artifacts and exercises real replication, restore,
negative data controls and parseable profiles. It does not execute long soaks,
expensive fault calibration, S3 provider comparisons, or live deployment checks.

Before integration, require all five PR checks: test, lint, compatibility and
both image checks. After an authorized deployment, the coordinator must verify
actual control/worker images and pinned identities, Fly health, fleet rollout
convergence, fresh attributed verification and historical incidents. A successful
notification only accepts asynchronous rollout work. A health endpoint proves
neither restored data correctness nor historical cleanliness.

Verify local profile manifests and parse artifacts, then independently verify
remote artifact and manifest delivery. Preserve upload failures after recovery;
retain the volume if evidence remains pending. Dashboard JSON in this repository
does not prove a live Grafana import: verify the intended host/organization,
datasource, panel queries and displayed identities. Never publish credentials,
environment dumps or unsanitized artifact bundles as documentation evidence.

## Interrupted worker provisioning

A pending worker's stored target does not prove that its machine exists. Fleet
reconciliation inventories actual machines and volumes, including resources
created before a controller lost the response or restarted. New provisioning
records a durable attempt ID, original deployment identity, and unique volume
name before sending creation requests. Resuming an older attempt keeps its original
image, deployment ID, and workload SHA even after a newer deployment is ready.
Late recovery events remain attached to that original deployment; unknown legacy
attribution is never inferred from the newest deployment. A matching resource is adopted only after checking its attempt, image,
workload identity, region, and volume ownership. A machine still being created
remains pending; a heartbeat arriving first does not prevent later completion of
the attempt.

An ambiguous creation outcome is never retried as another create request.
Missing inventory, conflicting ownership, multiple matching resources, and
retained legacy volumes leave provisioning unresolved for operator accounting.
A legacy pending worker with no conflicting resources can receive fresh resources
after machine and volume inventory succeeds and its previous machine is confirmed
missing or destroyed. Reconciliation does not delete volumes or replica prefixes.
A deployment replacement refuses teardown while a provisioning attempt remains
unresolved.

The event journal retains the interruption, safe failure classification (including
HTTP status, timeout, or cancellation when known), and subsequent recovery as
separate evidence. Provider response bodies and machine environments are not
persisted in these diagnostics. Worker activation and completion events commit
atomically. Recovery never erases the incident or establishes clean soak coverage;
operators must still verify the current physical identity, fresh heartbeat, and
restore results after deployment. The exact interruption point of a legacy
attempt remains unknown unless independent evidence establishes it.

### Retiring volume inventory

Fly's volume inventory includes records that are being deleted or are soft-deleted.
The default `fly volumes list` hides deletion-state records; use `--all` for
resource accounting. An earlier default-list absence is incomplete evidence,
not proof that the provider record no longer exists. See the official
[volume states](https://fly.io/docs/volumes/volume-states/),
[CLI listing options](https://fly.io/docs/flyctl/volumes-list/), and
[client-side filtering](https://github.com/superfly/fly-go/blob/main/flaps/flaps_volumes.go).

Legacy recovery can proceed past `pending_destroy` and `scheduling_destroy`
records only when they have no machine or allocation attachment and no actual
non-destroyed machine mounts them. Stale IDs in the pending worker row do not
count as active attachments. The previous machine must still be confirmed missing
or destroyed. Recovery retains the old volume ID, name, state, region, size,
attachment references, and creation time in the event journal, together with the
original worker/machine identity. It then creates a uniquely named volume and
uses a new volume-specific replica prefix without touching the retiring records.

Retiring volumes are never adopted. Adoption requires `created` or `hydrating`
state and the existing ownership, configuration, and immutable-target checks.
Unknown states, live same-name records, conflicting attachments, or active machine
mounts remain ambiguous. A retiring volume belonging to a current durable attempt
also leaves that attempt unresolved; recovery does not discard its creation
intent or issue another create request. Other provider deletion/transition states
are conservatively blocked. Every observed matching volume's safe metadata is
retained, and recovery never turns the original interruption into a clean run.

## Control deployment metric refresh health

Worker reports persist their complete evidence before acknowledgement. They
schedule deployment gauges separately, so comparison reads do not retain the
per-worker report lock. One observer coalesces pending sources and runs at most
one refresh at a time, with a five-second interval after each attempt and a
ten-second read budget. Failed work remains pending for retry. Startup metric
hydration also has a ten-second read budget. Shutdown cancels and waits for the
observer before closing the control database.

Deployment rollout and comparison gauges publish only after all snapshot reads
succeed. A failed or cancelled refresh keeps the previous complete snapshot;
old values are not evidence of current health. Inspect:

- `soak_control_deployment_refresh_healthy`: latest attempt succeeded (1) or
  failed (0).
- `soak_control_deployment_refresh_last_success_unixtime`: freshness of the
  last complete snapshot.
- `soak_control_deployment_refresh_last_attempt_unixtime`: latest completed
  attempt, including failures.
- `soak_control_deployment_refresh_failures_total`: process-lifetime failures,
  retained after successful refreshes.
- `soak_control_deployment_alert_refresh_failures_total{source}`: failed alert
  reads; one failed or absent source does not skip the other queued sources.

Use counter increases across process restarts and retain incident observations
alongside recovered health. Refresh coalescing affects derived gauges only;
verification, event, and runtime evidence history is neither capped nor deleted.
