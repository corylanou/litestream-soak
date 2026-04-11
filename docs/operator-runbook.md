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

The control plane is protected with HTTP basic auth. Keep the username and
password in your local `.envrc`.

Grafana:

- import `grafana/soak-dashboard.json`

## What To Look At First

### Control Plane Home

Start on `/ui`.

The home page is the fastest answer to "is anything wrong right now?" It shows:

- total workers, healthy workers, and workers needing attention
- an incident spotlight for the most urgent recent failure
- a worker table with status, heartbeat age, last check, and profile
- a failure queue with recent failed verifications
- an event feed

If the home page shows a worker as `degraded`, open that worker first.

### Worker Detail Page

Open `/ui/workers/{id}` for the failing worker.

This page is the incident page. It gives you:

- worker identity and workload shape
- last heartbeat and current status
- last verification result
- latest failure plus classified failure stage and signature
- recent verification history
- recent event history
- Fly machine metadata
- a copyable AI prompt bundle

### JSON Endpoints

These are the fastest machine-readable views:

- `/api/worker-summaries`
- `/api/failures`
- `/api/workers/{id}`
- `/api/workers/{id}/incident`
- `/api/workers/{id}/prompt`

Use `/api/worker-summaries` to understand fleet posture in one request. It
includes workload config, last verification, latest failure, classified failure
stage/signature, and triage commands.

Use `/api/workers/{id}/incident` when you want the full bundle to inspect or
hand to an LLM.

Use `/api/workers/{id}/prompt` when you want a copy-paste triage prompt
immediately.

## How The Control Plane Helps Debug

The control plane helps in four ways:

1. It keeps the latest worker workload next to the failure, so you can see
   whether the problem came from a synthetic, replay, or mixed worker.
2. It classifies the latest failure into a failure stage and failure signature.
3. It preserves recent verification history so you can tell whether the worker
   is stuck, flapping, or recovered.
4. It gives exact next-step commands for the affected Fly machine.

The incident prompt is built from the worker, workload, latest failure, recent
verifications, recent events, machine metadata, and triage commands in
`internal/orchestrator/api.go`.

## How Grafana Helps Debug

Grafana is the fleet-level lens. Use it to answer "how broad is this?" before
you drill into one machine.

The dashboard is strongest at:

- fleet posture
- current verification freshness
- sync age drift
- worker restarts and Litestream restarts
- workload shape comparison
- current failure labels
- last failure labels even after a worker recovers

The key dashboard panels are:

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
   - last heartbeat
   - last verification duration
5. Compare against Grafana:
   - is the failure clustered by profile?
   - is it clustered by replay dataset?
   - are sync age or restart counters also moving?
6. Copy the prompt bundle or incident JSON.
7. Run the triage commands from the worker page or API response.

## Standard Triage Commands

Every worker summary and incident bundle includes commands like:

```bash
fly machine status <machine-id> -a litestream-soak
fly logs -a litestream-soak -i <machine-id>
fly ssh console -a litestream-soak
curl -sS https://litestream-soak-ctl.fly.dev/api/workers/<worker-id>/incident | jq .
```

Inside the worker machine, these checks are usually the most useful:

```bash
ps aux | rg litestream
ls -lah /data
ls -lah /data/litestream.sock
tail -n 50 /data/verification.log
cat /data/litestream.yml
sqlite3 /data/test.db 'pragma wal_checkpoint(passive);'
```

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
- verification status, duration, summary, and error text

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
2. Import the Grafana dashboard from `grafana/soak-dashboard.json`.
3. Learn the three main endpoints:
   - `/ui`
   - `/api/worker-summaries`
   - `/api/workers/{id}/incident`
4. Learn the three main failure families:
   - restore failures
   - integrity-check failures
   - `/sync` socket or timeout failures
5. Practice one investigation with:
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
