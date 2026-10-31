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

## Control evidence database

The control plane compacts cumulative profiling history from `evidence_runtime`
in eight-row transactions. It records its cursor in
`evidence_compaction_progress`, so a restart resumes at the next row. Look for
`Runtime evidence compaction progress` and `Runtime evidence compaction complete`
in ctl logs. The hourly retention pass deletes evidence from archived
deployments only after two newer deployments for that source exist and the
entire runtime window is older than the retention window. It preserves the
current deployment and the previous deployment used by the latest comparison,
including main soak history needed by scorecards and lifecycle checks.

Compaction frees SQLite pages for reuse, but an existing database created with
`auto_vacuum=NONE` does not return those pages to the volume. After compaction
finishes, inspect `PRAGMA page_count`, `PRAGMA freelist_count`,
`PRAGMA auto_vacuum`, and `df -h /data`. If `auto_vacuum` is `0`, schedule an
offline database rebuild with the ctl process stopped and a verified backup
outside the volume. Confirm that free volume space exceeds the current database
size plus a safety margin before running:

```bash
sqlite3 /data/soakctl.db 'PRAGMA wal_checkpoint(TRUNCATE); PRAGMA auto_vacuum=INCREMENTAL; VACUUM; PRAGMA auto_vacuum;'
df -h /data
```

The rebuild holds an exclusive SQLite write lock, so do not run it while ctl
accepts worker reports. It is a deliberate one-time step for existing volumes.
New databases start in incremental vacuum mode. Once an existing database has
been rebuilt, the retention loop releases up to 128 free pages per pass and
logs the page count, free-list count, and vacuum mode.

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

The list returns archive metadata and summaries only. Add `&payload=true` to include each archive's full evidence payload (often over 1 MB each), or fetch one archive with `/api/run-archives/{id}`.

## Object-Store Cost Meter

Workers report cumulative object-store counters in every heartbeat. The meter keeps three kinds of numbers apart because they mean different things:

- Real replica HTTP requests and body bytes (`replica_requests`, `replica_request_bytes_sent`, `replica_response_bytes_received`) come from the S3 observe proxy, which sees every request Litestream's replicate process sends, with ListObjects counted as `LIST`. They are only present for workers that opt in with `S3_OBSERVE_PROXY_ENABLED` (or the fleet-wide `SOAK_ENABLE_S3_OBSERVE_PROXY`). The observe proxy re-signs forwarded requests for the target host, and if it cannot start (for example, missing signing credentials or region) the worker logs a warning and replicates directly instead of failing. Fail-open covers startup only; once the proxy is running, an upstream failure reaches Litestream as a 502. The re-signing path does not support signed aws-chunked payloads (current Litestream disables them for custom endpoints). In observe mode the verifier restore, pprof/evidence uploads, and the s3cmd replica-level and debug-snapshot listings go straight to the target endpoint, so the proxy only sees replicate traffic and every request is counted once; those diagnostic listings are not metered.
- Logical object operations (`litestream_object_operations`, `litestream_object_bytes`) come from `litestream_replica_operation_total` and `litestream_replica_operation_bytes`. Litestream increments these per uploaded or deleted object, not per HTTP request: GET and LIST never appear, a multipart upload counts once, and a batch delete counts every object. Use them for trends on workers without the observe proxy, not for request billing.
- Verifier restore requests (`restore_requests`) are parsed from the restore's `s3 request` debug lines. Evidence uploads (`evidence_object_uploads`, `evidence_upload_payload_bytes`) count successful pprof/evidence `s3cmd put` calls and their payload bytes; failed attempts and multipart part requests are not counted.

The control plane turns the counters into deltas. Each worker process sends a counter epoch, and Litestream counters carry their own epoch (`process_start_time_seconds`, or the Litestream PID when that series is absent), so a restarted worker or Litestream process starts a new baseline even if its first scrape is late. Data is stored as hourly per-worker buckets plus peak `data_disk_used_bytes`. Buckets older than 30 days are pruned on the hourly retention pass.

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/cost-meter?source=main&since=7d" | jq .
```

`since` accepts a duration (`6h`, `7d`) or an RFC3339 timestamp and defaults to 24 hours; the window starts at the top of the hour containing `since`. Omit `source` for every source. Each source reports summed totals, the largest single-worker peak disk, `worker_peak_disk_used_bytes_sum`, and per-worker totals. Prometheus series: `soak_control_source_object_store_requests_total{origin="replica"|"restore"}`, `soak_control_source_object_store_request_bytes_total{direction="sent"|"received"}`, `soak_control_source_litestream_object_operations_total`, `soak_control_source_litestream_object_bytes_total`, `soak_control_source_evidence_object_uploads_total`, and `soak_control_source_evidence_upload_payload_bytes_total`. Stopped workers keep their identity, status, verification, failure, and platform-event series (the Compute State panel still shows them as stopped) but stop exporting resource measurements such as disk, sizes, lag, and process stats.

### Metering Window

`SOAK_METERING_WINDOW_SOURCES` (control-plane env, comma-separated source names, empty by default) runs a scoped metering window. Every fleet-spec worker of a listed source gets the observe-mode S3 proxy and a `metering_window` marker in its workload config (passed to the worker as `SOAK_METERING_WINDOW=true`, so it is part of the attested run identity); profiles that already use a fault-injection mode keep their mode. Unlisted sources are untouched, and with the setting empty the fleet spec is unchanged. Exclusion follows the run, not the clock: verifications, incidents, and runtime evidence from a run whose profile carries the marker are excluded from release-quality scoring, RC-versus-main comparison, failed-source pause, and dormancy, whenever they arrive (scorecards report `metering_window_excluded`). Runs started before the window opens or after it closes count normally. Separately, on startup the control plane records a window start for each newly listed source and closes the window of any source no longer listed, in the `metering_windows` table; that table drives the dashboard home and worker page "Metering window: not test evidence" banner and the cost-meter report. `/api/cost-meter` reports each source's `metering_window` with `active`, `started_at`, and `ended_at`. Clearing the setting returns workers to direct replication at the next roll.

## Worker Startup And Populate

A single-database worker seeds `/data/test.db` with `litestream-test-populate populate` before it writes `litestream.yml` and starts Litestream. The worker image builds two binaries from the same pinned `WORKLOAD_SHA`: `litestream-test`, the load binary, is built with `SQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0` so that Litestream owns checkpointing once it runs; `litestream-test-populate` is the same source with SQLite's default automatic checkpoint, because populate runs before Litestream exists and nothing else may touch the database while it writes. SQLite then checkpoints inside populate's own connection after each commit once the WAL passes 1000 pages, with no second writer and no lock contention (an earlier harness design ran a concurrent checkpointer that took the write lock, which exceeded populate's 5-second busy timeout on Fly volumes and failed every gate-large-db populate with "database is locked"). The only thing running beside populate is a watcher that stats the WAL file every 100 ms and never opens the database: if the WAL reaches the 512 MiB ceiling the watcher kills populate, the partial seed is removed, and the worker reports the pre-start failure instead of filling the volume. Measured with the pinned workload source at `512MB` and both indexes: the populate build peaked at 8.3 MB of WAL and exited 0 in 33 s with 735 MB of database; the load build, run the same way, reached 543 MB of WAL in 3.8 s and was killed at the ceiling. At the gate-large-db seed (`6GB`, both indexes) the populate build produced an 8.83 GB database with the same 8.3 MB peak WAL and exited 0 in 11m40s, so peak disk during populate is the database plus about 8 MB. Without any bound the same seed needed 2.1 GB of WAL, and the `6GB` gate-large-db seed needed 36.5 GB of WAL and 45.3 GB in total, which is how the rc-gate 1 candidate filled its 40 GB volume. If `litestream-test-populate` is missing from PATH (a local rig built from the Makefile before it added the second binary), the worker falls back to the load binary with a warning and the ceiling watcher is the only bound. After populate exits the worker runs one `TRUNCATE` checkpoint with nothing writing, so the seed is a single file before Litestream starts. populate gets two attempts, each from a clean database, and every failure is reported as a `worker_failed` event (message prefixed `populate:`). Gate fleets alone set `populate_stop_on_failure` (`POPULATE_STOP_ON_FAILURE=true` on every rc and upgrade gate worker); on such a worker, if both attempts fail the same way (identical error, or both a WAL ceiling kill or a full disk) without the run being cancelled, the worker exits cleanly after reporting, so Fly does not restart it ten times and the gate sees a single harness failure instead of a crash loop. Main sentinels and PR workers do not set the flag, and a gate worker whose two attempts fail differently is treated as transient; both keep the normal exit-1, restart-on-failure behaviour, as do the many-db, churn and FTS populate paths and later startup failures.

`litestream-test populate` has no seed flag. It picks each index (`timestamp`, `int_field`) with its own `math/rand` draw at the default `-index-ratio 0.2`, and a random-integer index multiplies the WAL populate writes. The worker now passes `-index-ratio` explicitly from `POPULATE_INDEX_RATIO` (workload `populate_index_ratio`, default `0`, so a seed has no indexes unless a profile asks for both with `1`; `gate-large-db` pins `1` at the profile level, so the candidate and baseline both seed the heavier indexed case, and the bound above still holds: an 8.83 GB indexed seed plus at most the 512 MiB ceiling is under 9.4 GB on the 40 GB volume, against the 100 MB peak measured) and runs populate with `GODEBUG=randautoseed=0`, which makes populate's global `math/rand` use the fixed `Seed(1)` sequence. Index draws and `int_field` keys are therefore identical on both sides of a gate; row payloads come from `crypto/rand` and timestamps from the wall clock, so the row bytes still differ per run. Only `0` and `1` fix the schema by construction; any ratio in between is deterministic through the seed but opaque.

Populate completion is recorded in `/data/.test.db-populated.json` (completion time, workload SHA, target size, index ratio, realized indexes, size, checkpoint stats). On restart a database with the marker is reused; a database without the marker that already has `/data/.test.db-litestream/` next to it predates the marker and is adopted (the marker is written with `adopted: true`); any other database without a marker is a populate that was cut short, and the worker removes `test.db`, `-wal`, `-shm`, and the marker before populating again, which also releases the disk the partial seed consumed. A failed populate removes its partial files and sends a `worker_failed` event whose message starts with `populate:` (for example `populate: aborted populate: wal is ... at or above the ... ceiling`), which harness scoring treats as a pre-start harness failure rather than a Litestream result. A newly provisioned worker is `starting`; heartbeats sent before startup completes carry `startup_phase` (`populate` while seeding, `litestream` while the config is written, Litestream starts, and the first sync is awaited) and keep it `starting`, and only the first heartbeat after the first Litestream sync flips it to `running`, so gate readiness and the verification clock begin only then. A failure in that window (`write litestream config:`, `start litestream:`, `wait for first sync:`) is reported as a `worker_failed` event the same way. A `starting` worker that stops heartbeating is flagged stale like a running one. `litestream.yml`, the replica target marker, and the upgrade lane state are written atomically (temp file, fsync, rename, directory fsync), so a write that fails on a full disk leaves the previous file intact instead of a 0-byte config.

## Release Candidate Gate

A release candidate gate is a bounded, paired soak: the candidate commit runs as source `rc-<cand8>-<suffix>` and the baseline commit runs as `rcbase-<base8>-<suffix>`, each with the same gate fleet. The suffix is the last eight hex digits of the workflow run id, so every dispatch owns distinct sources (worker names, image labels, replica prefixes, archives) and a second gate for the same commits can register and run while an earlier one is still open; re-running the same workflow run keeps the same suffix, so registration stays idempotent. Gates registered before the suffix existed keep their `rc-<cand8>`/`rcbase-<base8>` names and still pass every gate-source check. Launch a gate with the manual `.github/workflows/soak-rc.yml` workflow. Inputs: `candidate_ref` (tag, branch, or SHA in `benbjohnson/litestream`; worker images and the compatibility check only fetch from that repository, so `repo_full_name` accepts nothing else), `baseline_sha` (defaults to the current upstream `main` head), `duration` (whole hours, default `72h`, maximum `168h`), `tail`, and `profiles` (optional comma-separated subset of `gate-stress`, `gate-manydb-500`, `gate-large-db`, `gate-remote`, `gate-vfs`; empty runs the default four, and the opt-in `gate-vfs` lane runs only when the subset names it). With a subset only those workers are provisioned on each side, readiness waits only for them, the ledger scores only their scenarios, and the registered resources and cost reflect only them; `tail` requires `gate-stress` in the subset. Because each dispatch is a new gate, check the ledger before launching so an accidental second dispatch does not double the fleet.

To rerun only the large-db pair for a candidate while its full gate is still open, dispatch the workflow with `candidate_ref` set to the candidate SHA, `baseline_sha` set to the same baseline the open gate uses, `duration` `24h`, `tail` `false`, and `profiles` `gate-large-db`. By hand: `GATE_PROFILES=gate-large-db GATE_SOURCE_SUFFIX=<suffix> scripts/register-rc-gate.sh benbjohnson/litestream <candidate-ref> <candidate-sha> <baseline-sha> 24h false`, then build and notify each side with the suffixed source names exactly as the workflow does.

The workflow resolves both refs to full 40-character SHAs before it builds anything, builds one worker image per side labelled `sha-<soak12>-<source>-ls-<litestream12>`, registers the gate with `POST /api/admin/rc-gates` (JSON fields `profiles`, an array, and `source_suffix`, 1 to 8 lowercase letters or digits, are optional), then sends a deployment-ready notification for each source. Registration is idempotent: repeating an identical request returns the open gate, and re-sending a side's notification for the same soak and Litestream SHAs reuses the build pinned first. A request for sources that already belong to an open gate with different settings is refused with 409. The control plane rejects a gate deployment that has no open gate, a Litestream SHA other than the pinned one, or a different soak build, so a gate never rolls to a new soak build mid-run.

The same check runs again whenever a gate worker is created, replaced, rolled, resumed from dormancy, or recovered from pending provisioning, under the source lock that teardown also holds. Provisioning is refused once a gate is closed, after a worker's deadline (the gate expiry, or the tail expiry for `gate-stress` with `tail`), for any profile other than `gate-stress` during the tail, and for any build other than the pinned one.

The gate loop records when each side is first observed with every selected worker running on its pinned build (`candidate_ready_at`, `baseline_ready_at`); a worker still populating reports `starting`, not `running`, so a side is not ready until its seeds are complete. The gate counts as paired only if both were observed ready before the 2-hour launch deadline and before the gate expires. If the deadline or the expiry passes first, including for a gate shorter than 2 hours, the gate is aborted: both sources are archived, every gate worker is destroyed, including partially provisioned ones, and the gate closes with status `aborted`. Readiness is observed every 5 minutes, so a side that becomes ready in the last few minutes before the deadline can still be aborted.

| Worker | Region | CPU / memory | Volume | Workload |
| --- | --- | --- | --- | --- |
| `gate-stress` | ord | 2 shared / 2 GB | 20 GB | alternating 6-hour phases of `overload-truncate0` and `pinned-reader`; two scenario databases of roughly 3.5 GB each plus WAL, and a restore needs that much again in temp space |
| `gate-manydb-500` | ord | 2 shared / 3 GB | 15 GB | 500 databases, directory mode, rotating active set |
| `gate-large-db` | ord | 2 shared / 4 GB | 40 GB | 6 GB seed with both populate indexes (`populate_index_ratio: 1`) and per-phase verifier budgets |
| `gate-remote` | ams | 1 shared / 1 GB | 10 GB | low volume |
| `gate-vfs` (opt-in) | ord | 2 shared / 2 GB | 10 GB | 10 writes/s with 30-second L1 compaction and 10-minute snapshots; two hydrated read-only Litestream VFS readers (`sqlite3` with `/usr/local/lib/litestream-vfs.so`) against the replica under `/data/vfs/` |

`gate-vfs` seeds a `cache_probe` table (2048 rows of 3500-byte payloads, about 8 MiB) next to the synthetic load and bumps one sixteenth of its rows every minute. The `snapshot` reader opens the replica through the VFS with `PRAGMA cache_size=-1024` and `mmap_size=0`. Each reader session starts from a fresh hydration (the hydration file, its `.meta` and the previous log are removed first) and is judged ready only when its own VFS log, written at DEBUG level to `/data/vfs/<name>/vfs.log`, carries the `hydration complete` line; `PRAGMA litestream_hydration_progress` cannot be used for that, because it reports the restore compactor's progress, which reaches 100 before the catch-up that precedes completion and stays 0 on the resume path, and a reader judged ready by it still fetches every page from the replica, so a probe scan takes minutes. The log is read incrementally for the rest of the session: a logged hydration failure, including the `cannot fetch new ltx files ... apply hydration updates` line the VFS writes when it turns hydrated reads off after a failed page apply, makes the cycle inconclusive (`vfs_hydration_incomplete`) and restarts the session. Both checks re-read the log after the reader catches up and again before judging a pass, waiting one poll interval plus a margin (3 s) first because the VFS writes that line only after the poll releases its file mutex. Known limitation: neither SHA exposes `hydrator.Complete()` to SQL (the pragmas are `litestream_txid`, `litestream_lag`, `litestream_time`, `litestream_hydration_progress`, `litestream_hydration_file` and `litestream_write_enabled`; the functions are `litestream_txid`, `litestream_time`, `litestream_lag` and `litestream_set_time`), and the poll loop suppresses the `cannot fetch new ltx files` line when the apply error wraps a context deadline or cancellation, so a request timeout can turn hydrated reads off without any marker and a cycle on that session can pass without exercising the hydrated reader. The baseline is the control for this: a `vfs-vfs-reader` result is only meaningful when the baseline side shows `vfs_snapshot_drift`, which proves its reader was hydrated under the same conditions. The DEBUG log records every page read, about 100 MiB an hour for the snapshot reader; it is never truncated in place, because the VFS could append a marker between a scan and the truncate, and a session whose log passes 256 MiB is dropped at its next cycle and replaced by a fresh one with a new hydration directory and log. The lane's write rate is deliberately low and shell statements get a 10-minute budget because the VFS applies every poll's page updates to the hydrated file under its file mutex, fetching each page from the replica one request at a time; under a heavier primary a poll holds the mutex for minutes, the reader's statements stall behind it, and the poller falls further behind on every round. A compaction that moves pages to a new level (the first L1 file after start, and every L2 and L3 file) makes the VFS fetch every page in that file again even though the hydrated copy already holds them, so a reader stalls for that file's page count times the replica round trip, and a cycle that meets one is inconclusive rather than failed. Each cycle holds the background probe writer, runs `BEGIN` and a full-scan aggregate of the probe table on the reader, and only then commits one probe update on the primary and calls Litestream's `/sync`, taking the returned txid as a position that holds the update and that the first read could not have seen. Still inside the transaction it waits for `PRAGMA litestream_txid` to reach that txid (a reader that never gets there within 10 minutes is inconclusive, `vfs_reader_not_caught_up`; a failed sync is inconclusive, `vfs_reader_unavailable`), then re-reads the same statement 16 times 15 seconds apart, runs `COMMIT`, and reads once more. Any re-read that differs from the first is a `vfs` verification with restore mode `vfs-snapshot` failing as `vfs_snapshot_drift` (the benbjohnson/litestream#1608 bug); a post-`COMMIT` read that does not equal the primary's aggregate after the update is inconclusive. The probe update refuses to run while the verifier holds the load paused, so a cycle that overlaps a verification waits for the pause to lift (up to 10 minutes, then inconclusive) and never writes inside a verification boundary. A statement that times out leaves late output in the shell's stream, so the session is dropped and recreated even when the cycle is only inconclusive. At every verification boundary the `read` reader waits for `PRAGMA litestream_txid` to reach the boundary and compares the probe aggregate and every table's row count with the quiesced primary; a difference is restore mode `vfs-read` failing as `vfs_read_mismatch`. A missing extension, a shell or statement failure, or a timeout is inconclusive (`vfs_reader_unavailable`), as is hydration that has not reached 100% within 10 minutes (`vfs_hydration_incomplete`). The ledger scores both modes in the `vfs-vfs-reader` scenario: a candidate failure is a regression whenever the baseline has none, failures on both sides are `both_failing`, and a baseline-only failure passes, which is the expected shape for a candidate that fixes a reader bug the baseline still has.

`gate-stress` never merges its stressors. The worker runs one scenario at a time in wall-clock-aligned phases (`STRESS_SCENARIOS`, `STRESS_SCENARIO_PHASE`), so the candidate and baseline machines run the same scenario at the same time. Each scenario has its own database directory and replica prefix under `scenarios/<name>`, Litestream restarts between phases, and every report carries the scenario in its run identity.

Gate sources are excluded from the 24-hour success teardown, PR max age, the upstream PR poller, and main and PR fleet reconciliation. Dormancy still applies. When the gate expires, the control plane archives the evidence for both sources and destroys every gate worker, including ones already marked stopped, through the same path as operator source teardown (archive type `teardown`, reason `rc_gate_ttl`). The gate closes only after every machine, volume, provisioning resource, and replica prefix is gone, or is retained by an [evidence hold](#evidence-hold); otherwise the loop retries every 5 minutes. With `tail` enabled, `gate-stress` keeps running in both sources until 7 days after launch, and its final archive uses type `rc_gate_tail`.

```bash
curl -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/rc-gates" | jq .
```

The read-only ledger lists every gate with its kind (`rc` or `upgrade`), pinned SHAs and images, start, expiry, tail expiry, launch deadline, status (`running`, `tail`, `completed`, or `aborted`), its `profiles` subset when one was requested, every machine with its size, volume GB, start, and expiry, and `held_workers` for any gate worker whose machine, volume, and replica prefix are retained by an [evidence hold](#evidence-hold). Each gate also reports candidate-versus-baseline results per scenario; a gate with a profile subset lists only the scenarios of those profiles. Only verifications attributed to the pinned deployment, reported by workers created for this gate, started inside the gate window, and carrying the scenario's identity count. Metering-window runs are excluded as they are everywhere else, and a gate source only joins a metering window when `SOAK_METERING_WINDOW_SOURCES` lists it. A scenario passes only when both sides have coverage and the candidate has no failures. Missing coverage is `incomplete`, never a pass. When a gate closes, its results are saved, and later evidence or retention does not change them.

## Upgrade Gate

An upgrade gate is the RC gate's small sibling for Litestream changes to sync and compaction that have to be proven against a replica an older build wrote, not just against a replica the candidate created from scratch. It reuses the RC gate machinery end to end: the same `POST /api/admin/rc-gates` registration with `kind: upgrade`, the same pinned builds, launch deadline, readiness pairing, ledger, exclusion from main and PR reconciliation and the 24-hour success teardown, and the same TTL teardown with archiving (archive type `teardown`, reason `rc_gate_ttl`). An [evidence hold](#evidence-hold) on an upgrade gate worker is honored by that teardown exactly as for an RC gate. The default TTL is `24h`. Upgrade gates do not offer `tail`, and nothing about them touches the metering window.

Launch it with the manual `.github/workflows/soak-upgrade.yml` workflow. Inputs: `candidate_ref` (the PR head or branch in `benbjohnson/litestream`), `baseline_sha` (the build the lane starts on; defaults to the upstream `main` head), `warmup` (how long the lane runs the base build before switching; whole minutes or hours, default `2h`, must end before the TTL), and `duration` (whole hours, default `24h`, maximum `168h`). The candidate source is `up-<cand8>` and the baseline source is `upbase-<base8>`. The candidate image is built with `--build-arg LITESTREAM_BASE_SHA=<base>` in addition to `LITESTREAM_SHA=<candidate>`, so it carries both builds: the pinned candidate at `/usr/local/bin/litestream` and the base at `/opt/soak/base/litestream-base` with its SHA in `/opt/soak/base/litestream-base.sha`. Images built without the argument get an empty `/opt/soak/base/`, and a lane worker refuses to start when the base binary or SHA is missing or equals the candidate SHA.

| Worker | Source | CPU / memory | Volume | Workload |
| --- | --- | --- | --- | --- |
| `gate-upgrade-lane` | `up-<cand8>` | 1 shared / 1 GB | 10 GB | base build for the warm-up with a 24-hour snapshot interval, then the candidate build on the same volume, database, and replica prefix with a 1-hour snapshot interval |
| `gate-upgrade-follow` | `up-<cand8>` | 1 shared / 1 GB | 10 GB | plain candidate: 50 writes/s, 15-minute snapshots, follow check |
| `gate-upgrade-follow` | `upbase-<base8>` | 1 shared / 1 GB | 10 GB | plain base: identical workload to the candidate follow worker |

All three run in `ord` with explicit L1/L2/L3 compaction intervals (30s/5m/1h), 10-minute L0 retention, replica level reporting, and the compaction progress check. The lane worker sets `UPGRADE_LANE=true`; its phase state lives in `/data/upgrade-lane.json` so a machine restart after the switch never runs the base build against candidate-written state, and the warm-up is measured from the lane's first start rather than from the latest restart. Litestream stops cleanly at the switch, the config is rewritten with the post-upgrade snapshot interval, and the candidate binary starts on the same `/data/test.db` and the same `S3_PATH`.

Every heartbeat, verification, and event from the lane carries `upgrade_phase` (`pre-upgrade` or `post-upgrade`) and `active_litestream_sha` (the build that was actually replicating) in its run identity. `litestream_sha` stays the image's pinned candidate SHA in both phases so deployment attribution and the gate pin check are unchanged. The profile hash is also unchanged across the switch; only the two phase fields differ. The phase marker is fsynced (file and directory) before the candidate binary starts, so a crash at the switch cannot leave a volume that would restart on the base build.

What the gate checks after the switch:

- Compaction progress. Each cycle's replica level report (`litestream ltx -level all`) is compared with the previous one. The cycle fails with `compaction progress check failed` when L0 holds more than `COMPACTION_L0_OBJECT_BOUND` objects (2000), L1 more than `COMPACTION_L1_OBJECT_BOUND` (6000), or L0's max txid advanced while no level from L1 to L8 advanced. The baseline report is captured right after Litestream's first sync, so a candidate whose compactor never runs on the legacy levels fails its first post-upgrade cycle. The verdict and the L0/L1/L9 counts appear as the `compaction_progress` step. The listing is parsed as a stream with no size cap (only the raw evidence tail is bounded), and the report is marked `complete` only when the whole listing was read and the command exited cleanly; a listing that failed, timed out, or is not complete is never judged. In that case an otherwise passing cycle is reported `aborted` with classification `replica_level_report_incomplete` (stage `compaction_progress`), the step shows `inconclusive`, the previous baseline is kept, and the follow check's L9 reading goes `pending` with `follow check inconclusive` instead of failing. The candidate-first-L9 note and the replica level metrics also use only complete reports.
- Restores from both sides of the switch. The normal restore rotation (`txid`, `timestamp`, `latest`) keeps running on the candidate build. In addition, every post-upgrade cycle restores the newest txid that was verified before the switch and runs the integrity check on it. That historical restore is reported as its own verification (`check_type: pre-upgrade-restore`, `restore_mode: txid`, steps `restore_pre_upgrade_txid` and `integrity_check_pre_upgrade`) rather than as part of the cycle, so when retention eventually removes the target the historical result turns inconclusive while the cycle's current-boundary restore and its compaction verdict keep counting. The ledger scores it in its own `upgrade-lane-historical` scenario, never as current `txid` coverage.
- Follow across an L9 boundary. See below; the lane enables the follow check only after the switch.

The follow check (`FOLLOW_CHECK=true`) runs on all three upgrade-gate workers. During a verification cycle, while the load is paused and the source boundary is known, the worker keeps one `litestream restore -f -follow-interval 1s` running into `test.db.follow` between cycles so the primary writes, compacts, and snapshots underneath it. The first cycle also runs the pinned variant, `restore -f -txid <boundary>`. Current Litestream rejects that combination (`cannot use follow mode with -txid`); the rejection is recorded as a `follow_pinned` step with status `unsupported` and `restore_mode_fallback: follow_pinned_unsupported`, and only the latest follow is covered. If a build accepts it, the pinned follow must reach exactly the boundary and match the source there. Each later cycle waits for the follow's `-txid` sidecar to reach the boundary, compares the followed database with the source's logical snapshot at that txid, and then checks the replica's L9 level. The result is reported as a separate verification with `check_type: follow` and `restore_mode: follow`: `passed` once the comparison matches, L9's max txid (or object count) advanced since the follow started or last passed, and the verified follower txid is at or past the new L9 snapshot's max txid (so the follower demonstrably applied data written after the snapshot), `pending` while it matched but no new L9 snapshot has landed or the follower has not yet been verified past it, and `failed` when the follow diverged, exited, or could not catch up; a failed follow is stopped and restarted on the next cycle. With 15-minute snapshots on the follow workers and 1-hour snapshots on the lane after the switch, the follow result becomes conclusive every cycle on the follow workers and every other cycle on the lane.

The ledger scores four scenarios for an upgrade gate. `upgrade-lane` compares the lane worker's post-upgrade current-boundary restores (per restore mode) with its own pre-upgrade restores, so the "baseline" is the same worker on the base build; follow and historical checks are excluded from it, so a historical pass can never stand in for current coverage. `upgrade-lane-historical` compares the post-upgrade `pre-upgrade-restore` results with the pre-upgrade `txid` restores, so a candidate that cannot restore base-written txids is a regression there. A report only counts for a phase when its `active_litestream_sha` is the build that phase must have run (the gate's base SHA before the switch, the candidate SHA after); a mislabelled phase gives neither coverage nor failures. `upgrade-lane-follow` compares the lane's post-upgrade follow verifications with the baseline follow worker's. `follow-l9` compares the two follow workers across the restore modes and `follow`; missing follow coverage on either side is `incomplete`. A regression anywhere fails the gate, as for RC gates.

Register one by hand with `scripts/register-rc-gate.sh <repo> <candidate-ref> <candidate-sha> <base-sha> 24h false upgrade 2h`, then send the two deployment-ready notifications exactly as the workflow does. The ledger entry carries `upgrade_warmup`, and the registered warm-up overrides the fleet default on the lane worker so the attested profile config matches what the worker runs.

Inconclusive verifications (status `aborted` or `pending`) count for neither side. On many-database workers a verification cycle restores and validates a batch of pending databases (at most `verify_changed_limit`, 100 on `gate-manydb-500`). The pending set holds every database the workload wrote to since its last successful verification plus, at start-up and after every worker restart, a seeded backlog of all databases so each is verified at least once; the written set is kept on the volume in `/data/dbs/.written-pending.json`, rewritten when a database enters or leaves it and recorded, with fsync, before the row that adds a database is committed (a second writer to the same database waits for that save, and a stalled fsync on the data volume holds writers and the verifier's pause the same way SQLite's own fsync on that volume would), and restored after a worker restart; when that file is missing or unreadable on a volume that already holds databases every database counts as written until verified, which the worker logs, and a save failure removes the stale file and syncs the directory so the removal survives a host crash, retries on every later write or acknowledgement, and records `many_db_written_state_dirty` once and `many_db_written_state_recovered` when a save succeeds again; when the save fails and the removal or its directory sync fails too the load's writes are refused with an error until a save succeeds, so a broken state disk shows as write failures instead of lost coverage; written databases are ordered ahead of the seeded backlog and fill the batch first. A clean batch is `pending` only when written databases did not fit; when only seeded backlog remains it is `passed`, and the summary and the `pending_coverage` step still report the backlog count and oldest age along with the batch size, how many databases were left out, and the mean and maximum per-database `sync` and `restore_validate` seconds, so a slow candidate shows up as longer per-database steps rather than as missing coverage. A batch that could not fit every pending database records a `many_db_changed_set_truncated` worker event with both counts. Many-database workers wait the plain `verify_interval` between cycles; the single-database rule of waiting at least twice the cycle's duration would rotate more databases into the pending set than the next batch can clear. The worker exports `soak_many_db_pending_count`, `soak_many_db_written_pending_count` and `soak_many_db_oldest_pending_age_seconds`. The worker reports a restore as inconclusive, and the control plane re-labels an older image's failure the same way on ingest, when the pinned `txid` or `timestamp` restore target is no longer available, or when the restore's object-store call ended with HTTP 408 `RequestCanceled` (`restore_s3_list_request_canceled`, `restore_s3_get_request_canceled`). Only that exact combination is inconclusive: a 403, a 500, a decode error without that object-store failure, a data mismatch, or the same 408 during `sync` all stay failures.

A gate worker that stops doing its job is a hard failure for its side even when it never reports a failed verification. Each side's result carries `reason` and `reason_detail`. The scoring reads the worker row (`status` `degraded` or `failed`, `error_message`, the last runtime snapshot's `data_disk_used_percent`) and the worker's events inside the gate window: `platform_disk_full`, `platform_disk_full_no_progress`, and `platform_disk_full_recovery_failed` give `disk_exhausted` (except a Fly disk-pressure line whose parsed path is the verifier's own restore output, `.restored`, `.restored.tmp`, `.restored.pre` or `.restored.pre.tmp`, which the platform log monitor no longer records and which the scoring ignores in already-stored events by reading the raw line from the event details; a path-less or unparseable line stays a fault, and a coalesced row is ignored only when every one of its occurrences was recorded with a restore-output path, which the event's `occurrence_keys` track per occurrence); a data disk at 98% or more in the last runtime snapshot is disk-full evidence on its own, even on a worker that is still heartbeating; two or more crashes across `platform_restart`, `platform_oom`, `platform_killed`, or `worker_failed`, or a single Fly `not restarting` exit, give `worker_crash_loop`; a `degraded` or `failed` status or a `worker_stale` event gives `worker_degraded`. Planned restarts are not crashes: a `worker_failed` whose message is a lifecycle message (`stress scenario phase ended`, `upgrade warm-up ended`, as older images report at a phase switch) is ignored and marks a planned restart. Every signal is normalized to one planned moment, on `gate-stress` the nearest wall-clock scenario phase boundary, and each moment forgives exactly one `platform_restart` within 10 minutes of it, however many duplicate lifecycle reports land on it; at a stress boundary a restart whose Fly message is a clean exit (code 0 or 143) can also claim that single allowance. Every other restart near a boundary, a second restart after the planned one, and any `not restarting` exit still count. Staleness counts only when the worker never heartbeated again or its heartbeats or verifications resumed more than 30 minutes after the `worker_stale` event; a `worker_stale` that hits three or more workers within 90 seconds is a control-plane restart, but that only excuses a worker that recovered within the 30 minutes. A `degraded` row whose `error_message` is `worker missed heartbeat deadline` counts only while the worker is still silent past the grace period. Platform log events that repeat the same message within 10 minutes are stored as one row whose `occurrences` column is what the crash count adds up; each occurrence is identified by the log line's own timestamp and payload, so the platform log monitor re-reading the same lines on its next pass does not count a crash twice, and a row recorded before occurrence tracking existed seeds its own stored timestamp and payload as its first occurrence. Events are read from the existing feed, so a gate already running when the control plane deploys is scored the same way, and an operator stopping a degraded worker afterwards does not erase the failure. A side with a hard fault is `failing` in every restore mode; both sides faulting is `both_failing`. Upgrade gates apply the same rules to their lane and follow workers; a scenario whose two sides come from one worker (the lane's pre- and post-upgrade phases) reports that worker's fault on both sides.

A fault that happens before Litestream ever started is a harness problem, not a candidate result, but the scoring needs affirmative evidence of that before it will say so. The side is reported as `harness_inconclusive` only when the faulted worker has no verification of any status inside the gate window, its row has no `last_runtime_at` (the control plane sets it only from a heartbeat whose `litestream_snapshot_healthy` is true), and either the persisted last heartbeat sample (`last_runtime_json`) shows `litestream_process.pid` 0, `litestream_snapshot_healthy` false with a `litestream_snapshot_error` (the IPC socket could not be polled), no Litestream uptime or TXID, and a live `worker_process.pid`, or the worker sent a `worker_failed` event from a pre-start step (`populate:`, `prepare disk-full recovery reserve:`, `write litestream config:`, `start litestream:`). `reason_detail` names the underlying fault and the evidence. That side keeps `no_coverage`, the scenario is `incomplete`, and it never counts as a candidate failure. Without that evidence, including a worker that died before it stored any sample, the fault stays a hard failure, as does any fault after Litestream reported once or after any verification.

### No-L9 lane variant

The base build writes an initial L9 snapshot at its first start, so the standard lane can never show a candidate taking over a replica with no snapshot level. The `nol9` variant closes that gap. Launch it from the same `soak-upgrade` workflow with `lane_variant: nol9`, or by hand with `scripts/register-rc-gate.sh <repo> <candidate-ref> <candidate-sha> <base-sha> 24h false upgrade 2h nol9`. It is its own small gate: the candidate source `up-nol9-<cand8>` runs one worker, `gate-upgrade-lane-nol9`, and the baseline source `upbase-nol9-<base8>` runs the base `gate-upgrade-follow` worker, so it never collides with a standard upgrade gate for the same commits and never needs to add a worker to one (which the pin rules would refuse).

`gate-upgrade-lane-nol9` is the standard lane plus `UPGRADE_DELETE_L9_AT_SWITCH=true`. At the switch, after the base Litestream process has exited (the Runner stops it before returning) and before the candidate binary starts, the worker lists its own replica prefix, deletes every object under a `0009/` level directory, lists again and refuses to start the candidate unless zero L9 objects remain. It never deletes while the base build runs, because the base build snapshots immediately whenever it sees an empty L9, and it refuses to touch any key outside its own `S3_PATH` (or `REPLICA_PATH` for file replicas): a listing that returns a foreign key aborts the purge before anything is deleted. Every attempt is recorded in `/data/upgrade-lane.json` under `l9_purge` with the prefix, the number of objects listed, every deleted key, the remaining L9 count, the attempt number, `succeeded`, and any error. Only a durable successful record lets the candidate start: a failed listing, delete, or verification is retried up to three times while the base build is still stopped, a crash between the phase switch and a successful purge repeats the purge on restart, and a restart that finds a failed or incomplete record purges again before the candidate starts. Once a successful record exists the purge never runs again, so L9 objects the candidate writes are never deleted. If every attempt fails the worker exits with `upgrade lane L9 purge: 3 attempt(s) failed` and sends a `worker_failed` event prefixed `upgrade lane harness failure`; the candidate is never started on a replica that may still hold base-written L9 objects.

After the switch the same checks run as on the standard lane: compaction progress (L0/L1 bounds and L1-L8 advancing while L0 does), the restore rotation, the historical pre-switch txid restore, and the follow check. The `compaction_progress` step additionally reports `candidate first L9`: the time and max txid at which the candidate's first L9 object was observed on the purged replica, persisted as `first_candidate_l9` in the lane state, or `not yet written since the switch purge`. With the lane's 1-hour post-upgrade snapshot interval the candidate's first L9 is expected roughly an hour after the switch; the follow check cannot pass before it exists.

The ledger scores the variant with its own scenarios so the standard gate's numbers are never mixed in: `upgrade-lane-nol9` (post-upgrade current-boundary restores against the lane's own pre-upgrade restores), `upgrade-lane-nol9-historical` (post-upgrade `pre-upgrade-restore` results against the pre-upgrade `txid` restores), and `upgrade-lane-nol9-follow` (the lane's post-upgrade follow verifications against the base follow worker). The active-SHA and phase rules are unchanged. Gate images are labelled `sha-<soak12>-up-nol9-<cand8>-ls-<litestream12>` and `sha-<soak12>-upbase-nol9-<base8>-ls-<litestream12>`.

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

## Evidence Hold

An evidence hold pins one worker's Fly machine, volume, and S3 replica prefix in place while an incident is investigated. While the hold is set, no control-plane path destroys them: gate TTL expiry, the gate launch abort, operator source teardown, the 24-hour success teardown, worker expiry, PR max age, upstream PR retirement, and operator or reconciler rolls, replacements, and dormant resumes all skip the worker. The hold is stored on the worker row (`evidence_hold_at`, `evidence_hold_reason`), so it survives control-plane restarts and deploys, and it can be applied to a worker that is already running, degraded, dormant, or stopped.

Apply a hold with a required reason:

```bash
curl -X POST -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/admin/workers/worker-rc-a25ac011-gate-large-db/evidence-hold?reason=large-db+restore+evidence+under+review" | jq .
```

A JSON body `{"reason":"..."}` with `Content-Type: application/json` works as well. The response reports the hold together with the machine, volume, and `replica_prefix` it protects; repeating the call is harmless and returns `already_held: true` with the original reason. The worker's `evidence_hold_at` and `evidence_hold_reason` appear in `/api/workers`, `/api/workers/{id}`, and worker summaries, and a gate worker appears under `held_workers` in `/api/rc-gates`.

What happens on a teardown while the hold is set:

- The source is archived as usual, and the archived worker record carries the hold fields.
- The held worker's machine is stopped if it is still running, its status becomes `stopped`, and the control plane records an `evidence_hold_retained` event naming the retained machine, volume, and replica prefix. Nothing is destroyed or cleared.
- Every other worker in the source is destroyed normally. Source teardown reports the held worker with outcome `held` and counts it in `held_workers`; it does not count as a failure, so the result stays `complete: true`.
- A gate still closes at expiry. The closed gate's saved results and the ledger list the retained worker under `held_workers`, so a held worker never keeps a gate open.
- Reconcilers do not restart, replace, or resume a held worker, and a fleet spec that names it will not recreate it. It stays stopped until the hold is released.
- Stale unattached volume GC resolves each candidate volume to its owning worker (by `fly_volume_id` or a provisioning attempt that references it) under that worker's lock, skips a held worker's volume with a `volume_gc_skipped_evidence_hold` event, and clears any persisted destroy retry for it. Volumes with no resolvable owner keep the existing GC behavior.

Release the hold when the evidence is no longer needed:

```bash
curl -X POST -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/admin/workers/worker-rc-a25ac011-gate-large-db/evidence-hold/release" | jq .
```

Release alone only clears the hold. If no teardown reached the worker while it was held, it keeps its previous status and the normal cleanup paths apply again on their next pass (worker expiry, PR max age, PR retirement, source teardown). If a teardown already ran while it was held, the worker is `stopped`: a closed gate is not revisited by any loop, the expiry and max-age loops ignore stopped workers, and a repeated `teardown-source` skips it as already stopped. In that case add `destroy=true` to release the hold and destroy the machine, volume, and replica prefix in the same synchronous request:

```bash
curl -X POST -sS -u "$SOAK_BASIC_AUTH_USERNAME:$SOAK_BASIC_AUTH_PASSWORD" \
  "https://litestream-soak-ctl.fly.dev/api/admin/workers/worker-rc-a25ac011-gate-large-db/evidence-hold/release?destroy=true" | jq .
```

The response reports `released`, `destroyed`, and any `destroy_error`. The earlier teardown archive already holds the evidence, so no new archive is written. Hold and release are recorded as `evidence_hold_applied` and `evidence_hold_released` events on the worker.

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
retrieved and space is freed. Each upload command has a 15-second deadline.
A retry pass attempts at most four records, each with sequential artifact and
manifest commands (at most 120 seconds of command time plus bounded process
cleanup). The separate uploader selects the least-recently-attempted pending
records first, using persisted `upload_last_attempt_at` (capture time for new records).
Historical failure counts do not penalize scheduling, and fresh arrivals do not
continually jump ahead of waiting retries. It wakes after capture and every
30 seconds, including after restart.
Successful artifact delivery is persisted as `artifact_uploaded=true`; a later
manifest retry resumes without resending the artifact. Shutdown cancellation
still applies to every command. Final capture and delivery share ten seconds,
with one second reserved for upload process cleanup. There is no automatic
expiry of failed uploads. This bounds disk use without deleting unuploaded
evidence. Capture itself is best effort; logs report failures and
queue/storage limits rather than treating missing evidence as success.

A delivered remote manifest reports `upload=uploaded`; the local manifest
stays pending until both artifact (when available) and manifest delivery
succeed. A failed attempt stores `upload_error` separately from capture errors,
with stderr limited to 4 KiB and configured credentials redacted. Successful
artifact and manifest delivery clears that current error. The first failure
and latest 15 failures remain in `upload_failures`, with UTC timestamps, attempt numbers, delivery
stage, and sanitized causes. New failures also record `kind` as
`deadline_exceeded`, `cancelled`, `subprocess_exit`, `provider_error` (an explicit
S3 error diagnostic), or `upload_error`, plus `exit_code` when a subprocess
exited. A killed subprocess is classified as a deadline only when its context
actually expired; historical kills without that evidence remain unattributed.
`upload_attempts` counts all attempts and
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
4. reserve the SQLite writer, re-sync and capture the matching source boundary
5. release the reservation and run `litestream restore -txid`, then independent integrity and logical comparison
6. resume the load generator

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

Before every restore, including the upgrade lane's pre-upgrade txid restore,
the verifier runs a preflight step (`restore_preflight`,
`restore_pre_upgrade_preflight`). It first removes any restore output an
interrupted run left behind (`.restored`, `.restored.tmp`, `.restored.pre`,
`.restored.pre.tmp` and their SQLite side files), then estimates the logical
size of the database a restore produces: the largest of the main file, the page
count in the database header, and the database size recorded by the newest WAL
commit frame, so a WAL that rewrites the same pages does not inflate the
estimate. That size plus 10% (at least 64 MB) is compared with the free space on
the volume holding the restore target. When space is short the restore is
skipped and the verification is `aborted` with classification
`harness_capacity_inconclusive` (stage `harness_capacity`), and the error names
the needed and free bytes. An ENOSPC raised while writing the verifier's own
restore output (`.restored`, `.restored.pre`, or their `.tmp` files) is
classified the same way, on current and older images. ENOSPC anywhere else, in Litestream replication or staging, the source
database, or its WAL, stays a `disk_capacity_full` failure and a worker fault,
so a build that bloats the disk remains visible. Inconclusive results count for
neither gate side and give no coverage.

A restore whose `ListObjectsV2` or `GetObject` call exhausted its retries with
HTTP 408 `RequestCanceled` is recorded as an inconclusive `aborted`
verification, not a failure, on both current and older worker images. It does
not count toward dormancy, worker degradation, or gate scoring, but it still
shows up in the worker's verification history with the original error, so a
provider that keeps canceling requests is visible without being blamed on the
build under test.

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

The many-DB tier ladder is no longer part of any default fleet. The
`SOAK_ENABLE_MANY_DB_FLEET`, `SOAK_ENABLE_MANY_DB_500`, and
`SOAK_ENABLE_MANY_DB_1000` flags are no longer read by the control plane, so
setting them does not add workers to `main` or PR fleets. The only always-on
many-DB lane is `sentinel-manydb` (`worker-main-sentinel-manydb`): 100
directory-discovered databases, 2 load workers, 10 GB volume, 2048 MB, 1 CPU,
with L0 retention checked every 2m and L1/L2/L3 compaction at 5m/15m/1h to
cut per-database S3 LIST traffic. The tier profile names below remain known to
the worker (`PROFILE=many-dbs-500-dir` and so on) for manual runs.

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
reconciliation activates the two sentinel profiles below for `main` and supported
`pr-N` sources. Explicit worker environment overrides can differ from fleet defaults;
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
| `sentinel-single` | Constant synthetic target 10, one generator worker, 1 KiB payload, Litestream default compaction and retention; inspect completed work and errors. | Baseline restore correctness and drift in default Litestream behavior; low offered load does not isolate every platform or harness failure. | Synthetic generator. |
| `sentinel-manydb` | 100 directory-discovered databases, two writer workers, rotating 2% active set; L0 retention check 2m and L1/L2/L3 compaction 5m/15m/1h. Inspect committed work, pending database count/age and restore outcomes. | Excluded from the legacy release score; relaxed maintenance intervals mean it does not cover default-cadence compaction at fan-out. | Directory discovery support; candidate interval support. |

The rows below were default profiles before the main fleet was reduced to the
two sentinels. They are no longer reconciled; they remain here to interpret
historical runs.

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
| `overload-truncate0` | Constant target 600, eight workers, 2 KiB payload, `truncate-page-n: 0`; inspect WAL/backlog and completed work. Litestream maps zero to its default TRUNCATE backstop (121,359 pages, about 500 MB at 4 KiB/page), as changed in [Litestream #1348](https://github.com/benbjohnson/litestream/pull/1348). | Restore check does not assert WAL boundedness or prove overload engaged; the profile checks the zero sentinel retains the default backstop under overload. | Candidate accepts `truncate-page-n: 0`; adequate disk. |
| `pinned-reader` | Constant target 200, four workers; companion transaction holds 4m with 45s pauses; inspect reader events and WAL/checkpoint observations. | Require actual reader engagement before attributing checkpoint effects; no query-latency guarantee. | Companion SQLite reader. |

Many-database profiles are excluded from the legacy release score;
they participate in retained complete-run reliability. All seed a
fixed set, rotate a 2% active subset, and use an in-process writer. Inspect
committed work, pending database count/age, individual restore outcomes, and
aggregate process/runtime samples. A successful batch is not full coverage;
untouched and deferred databases remain obligations. These profiles do not
exercise dynamic tenant creation/removal. Directory mode requires candidate
directory discovery support; list mode requires explicit database configuration.
Neither mode supplies native passive S3 LIST counts.

| Retired tier profile | Former activation (flags no longer read) | Actual workload and assertion | Specific limitation/capability |
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
GOTOOLCHAIN=go1.26.10 go build ./...
GOTOOLCHAIN=go1.26.10 go test ./...
GOTOOLCHAIN=go1.26.10 go test -race ./...
GOTOOLCHAIN=go1.26.10 golangci-lint run
GOTOOLCHAIN=go1.26.10 govulncheck ./...
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
Destroying a worker, including source teardown, closes its open provisioning
attempt. It first inventories the app and destroys any live machine or volume
the attempt owns, then marks the attempt `abandoned` and records
`worker_provisioning_abandoned`. If inventory or cleanup fails, the attempt
stays open and the worker is not marked stopped, so teardown remains retryable.
A relaunch of the same source can then provision fresh attempts.
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
