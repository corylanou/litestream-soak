# Offline backlog and constrained recovery

`offline-backlog` is an opt-in one-shot comparison for issue #223. It does not
add or activate Fly workers. Run the same controls against immutable release,
main, and candidate Litestream SHAs, retaining each result independently.

The schedule has five equal windows: idle, object-store outage (HTTP 503), high
latency (one quarter-window per request), throttling (HTTP 429), and reconnect
with sustained writes. Default windows are 10 seconds. Faults apply to actual
Litestream S3 requests through the existing rig proxy; they are not synthetic
success counters. Seed replication completes before the schedule starts.

## Run

For an existing local emulator accessible from Docker Desktop:

```bash
ONE_SHOT_PROVIDER_CLASS=local-emulator \
ONE_SHOT_SKIP_EMULATOR_START=1 \
S3_ENDPOINT=http://host.docker.internal:19150 \
S3_BUCKET=rig215 \
ONE_SHOT_RECOVERY_CONTAINER=1 \
ONE_SHOT_CPUS=0.5 \
ONE_SHOT_MEMORY=128m \
ONE_SHOT_TMPFS_SIZE=64m \
ONE_SHOT_RECOVERY_PHASE_SECONDS=3 \
ONE_SHOT_RECOVERY_WRITE_RATE=60 \
./scripts/local-rig-one-shot.sh offline-backlog \
  4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3 1
```

Without an endpoint override, the launcher starts its existing local MinIO
services and labels evidence `local-emulator`. An explicit endpoint requires
`ONE_SHOT_PROVIDER_CLASS=local-emulator` or `real-provider`. Real-provider runs
use separately supplied credentials and never start MinIO. Do not combine their
results with local-emulator runs: region, provider, retry policy, controls, and
pinned SHA must match within a comparison. No real-provider results are claimed
by the local fixtures.

Omit `ONE_SHOT_RECOVERY_CONTAINER=1` for host execution. Host CPU/memory controls
are unsupported when cgroup limits cannot be observed; disk quotas are not
claimed for ordinary host directories. Setting Docker limit variables alone
does not constrain host execution. Container runs read actual cgroup CPU and
memory limits and identify the longest matching mount for the fixture directory.
The CPU quota is quota/period; memory is bytes; tmpfs reports its size option.
A cgroup `max` value is explicitly unenforced. Tmpfs consumption also counts
against container memory, so disk and memory limits are not independent.

## Workload controls

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `ONE_SHOT_RECOVERY_PHASE_SECONDS` | 10 | Duration of each schedule window |
| `ONE_SHOT_RECOVERY_WRITE_RATE` | 600 | Aggregate target rows per second |
| `ONE_SHOT_RECOVERY_WORKERS` | 8 | Paced writer goroutines |
| `ONE_SHOT_RECOVERY_PAYLOAD_BYTES` | 2048 | Bytes per inserted row |
| `ONE_SHOT_RECOVERY_TRUNCATE_PAGE_N` | 0 | Existing overload truncate setting |
| `ONE_SHOT_RECOVERY_PIN_HOLD_SECONDS` | 4 | Existing pinned-reader hold; 0 disables |
| `ONE_SHOT_RECOVERY_PIN_PAUSE_SECONDS` | 1 | Existing pinned-reader pause |
| `ONE_SHOT_CPUS` | 1 | Docker CPU quota |
| `ONE_SHOT_MEMORY` | 512m | Docker memory and combined memory/swap limit |
| `ONE_SHOT_TMPFS_SIZE` | 384m | Docker fixture filesystem limit |

This reuses the fleet pinned-reader implementation and overload defaults
(600 rows/s, eight goroutines, 2 KiB rows, truncate threshold zero). The fixture
serializes commits and source sync to map exact row boundaries to replicated
TXIDs. Effective writer concurrency is therefore **one**, reported explicitly;
it is not an eight-connection contention benchmark. Replica sync runs separately
so write production continues during an object-store outage. Missed pacing ticks
are not fabricated into completed writes.

## Evidence and verdicts

Each request retains phase, method, object path, status, injected-response flag,
SDK attempt header, elapsed time, and duration. Query strings and authorization
headers are excluded. Injected 503/429 responses are counted separately from
unexpected HTTP errors returned by the provider or proxy, including those hidden
by a later successful SDK retry. Each source/replica sync failure, write failure,
restore attempt, and warning/error log survives recovery.

Samples record committed and replicated row boundaries, lag in rows, fixture
file bytes, and process peak RSS. Maxima are sampled every 100 ms, not continuous
filesystem peaks. RSS includes the harness; tmpfs/cgroup memory includes more
than process RSS. Replication throughput and **net backlog drain** are separate:
net drain subtracts continuing writes from replicated progress.

`drained_while_writing` requires positive committed-row growth and either a smaller
backlog or a fully caught-up boundary at the end of the reconnect window. Final sync runs after producers
stop, and `recovered` plus `validated_committed_rows` report that separate final
recoverability check. Final recovery alone cannot earn a catch-up verdict.
Every restored ID, value, storage type, and single-row transaction boundary must
match the committed source through the final boundary, using the existing rig
prefix oracle from #215/#204.

A success requires fault engagement, pinned-reader engagement when enabled,
observed backlog, drain under continuing writes, and valid restored data.
`inconclusive` means required exposure or drain was absent. Operation failures,
unexpected HTTP failures, SDK retries, injected HTTP failures, or warning/error
logs prevent a clean success, even if data subsequently recovers
(`recovered_with_incidents`). Expected injected HTTP failures retain their
separate category but do not constitute clean no-failure evidence. Deferred
finalization rechecks requests after shutdown and includes WARN/ERROR logs. External cancellation is `aborted`.

Incremental request, operation, and sample journals and Litestream logs are
written outside the constrained tmpfs in container mode. The launcher also
retains container exit/OOM metadata. An OOM kill or missing final result is an
incomplete run, never a pass. Containers and replica prefixes are retained;
cleanup and fleet activation are explicit operator responsibilities.

The retry and drain measurements address #187's regional 408 incidents and
#192's conversion of fast failures into long sync waits. They do not establish
that either regional incident is fixed. Metrics preserve failures as required
by #214; an improvement in catch-up throughput cannot cancel a correctness or
availability regression.

## Local validation

The pinned SHA above was exercised against local MinIO on September 9, 2026.
A race-enabled host fixture engaged all three faults and the pinned reader,
validated its final boundary, demonstrated drain while writing, and retained
its operation failures. A constrained fixture observed a 0.5 CPU quota, 128 MiB
memory limit, and 64 MiB tmpfs at the actual fixture mount, with no OOM kill.
It validated 625 committed rows, recorded 183 writes during drain, and measured
approximately 126 net drained rows/s. Seven injected HTTP failures, two
unexpected HTTP failures, four SDK retries, and eight failed operations were
retained; its outcome was `recovered_with_incidents`, not pass.

These are harness evidence, not release calibration or real-provider results.
Resource exhaustion, multi-version statistical separation, and regional-provider
comparisons remain unexecuted. Negative tests cover growing backlog, absent
continuing writes, hidden SDK retries, unexposed faults, invalid controls,
cancellation, and fixture paths outside the constrained mount.

A repeat with a three-second phase and four-second reader hold lacked outage
engagement and drain evidence; its fixture assertions failed despite final
recoverability. That failure is retained. The positive race fixture uses a
one-second hold within its three-second phases. Short windows are not a
calibrated release gate. Metrics sampling uses a separate lock so source-sync
waits do not block observations, and samples after the drain window are excluded.

The repository lint gate passes. An additional generated-harness lint scan
reports eleven diagnostics in the existing shared main template; the starting
revision produces the same eleven. No new recovery-template diagnostics were
reported. Those pre-existing findings are outside this issue.
