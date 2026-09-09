# Queue and expiring cache churn

These scenarios are opt-in. Neither is in the default fleet. The accompanying
queue.json and cache.json are workload configurations for an explicitly selected
DesiredWorker/FleetSpec; they round-trip through worker environment generation.
They are configuration artifacts, not fleet activation requests. The coordinator
owns any future reconciliation, rollout, and deployment decision.

For a fresh local database, select a mode and fixed configuration before starting
the worker with the usual local replica settings:

```sh
export LOAD_MODE=queue
export CHURN_CONFIG='{"slots":1024,"hot_percent":80,"workers":1,"rate":100,"payload_size":256,"seed":7}'
export GOTOOLCHAIN=go1.25.13
go run ./cmd/soakworker
```

Use LOAD_MODE=cache for expiration churn. Use a dedicated database for each
configuration; do not shrink the slot limit on an existing database. Churn
initialization replaces the synthetic population step, so INITIAL_SIZE does not
apply. Existing data is never deleted at startup. Invariant checks fail if
existing churn rows exceed the configured key range.

Configuration defaults are 1024 slots, 80 percent hot traffic, one worker,
100 operations/second, 256 payload bytes, seed 1. CHURN_CONFIG overrides individual
fields. Fleet configurations should specify all fields. Valid ranges are slots
1..1000000, hot_percent 0..100, workers 1..64, rate 1..100000, and payload_size
1..65536. The requested rate is an upper target; database contention and timer
resolution may reduce throughput. Compare workers=1 and workers=4, and
hot_percent=0 and hot_percent=80, holding the other values fixed.

The queue offers enqueue, claim, retry, claim, complete, delete, enqueue, expire,
and delete in that order for each deterministic key choice. Claim increments
attempts; retry returns a claimed job to ready. Complete atomically changes the
job state and writes its receipt. Delete atomically removes terminal jobs and
receipts. Expire marks unfinished jobs expired. At most slots jobs and slots
receipts exist. Cache operations offer two upserts then an expiration sweep.
Expiration uses logical operation ticks with TTL 8, not wall time. At most slots
cache entries exist. Values include their logical write tick. Payloads and key
ranges are bounded; SQLite file and WAL size are not strict physical disk caps.

The seed and configuration reproduce the offered key/operation sequence. They do
not reproduce concurrent commit order, lock errors, or exact final contents.
Conditional transitions can be no-ops when concurrent operations overtake one
another. Verification pauses wait for active transactions to finish; pending
operations may be skipped while paused, and no catch-up burst is scheduled.
The sequence restarts on worker restart. Use one worker and a fresh database for
a sequential comparison; the source snapshot remains the truth for each restore.

Metrics retain each failed attempt even when later operations succeed:

- soak_churn_mutations_total counts committed affected rows, including receipts;
  no-ops and rolled-back transactions add zero.
- soak_churn_operation_seconds measures every transaction attempt, including
  SQLite lock waits, excluding pacing. Its count is the attempt count.
- soak_churn_errors_total counts failures with kind busy (SQLITE_BUSY/LOCKED,
  including extended codes) or other. There is no automatic retry that erases
  failures; the queue retry operation is an application state transition.

All metrics carry mode, operation, worker_id, profile, and source. Error counters
also carry kind. Prometheus metrics are process-lifetime counters. Heartbeats,
verification reports, and workload_error events additionally carry
workload_counters_present, workload_counter_epoch, workload_attempts_total,
workload_mutations_total, workload_busy_total, and workload_errors_total.
Errors includes busy; other errors equals errors minus busy. Counter epochs are
unique per process, including restarts that reuse a machine/run ID. The retained
run-evidence consumer groups by immutable identity and epoch and uses cumulative
maxima, so retrying delivery cannot double-count failures.

Each failed attempt receives an immutable workload_event_id (epoch:attempt),
operation, mode, error kind, latency, original error message, and cumulative
counters. Before delivery, the worker atomically writes and fsyncs that event
under DATA_DIR/churn-outbox. No pending event is overwritten. The outbox holds at
most 1024 files or 16 MiB; reaching either limit or failing persistence stops the
workload and marks evidence unavailable rather than dropping old failures.

A separate uploader retries pending events on notification and every ten seconds,
including after restart, with the original worker/run identity and exact event
ID. Successful delivery removes only the acknowledged record. Delivery is at
least once: consumers deduplicate event IDs and take cumulative maxima per epoch.
Network delivery never holds the counter/snapshot lock and does not block
heartbeat collection. Local persistence can slow failed workload attempts.

The control-plane retention/comparison consumer is delivered by issue #211.
Retained volumes preserve unsent evidence; destroying a volume before delivery
cannot preserve its unreported tail, so missing epochs/coverage must remain
unknown rather than clean. No fleet or volume destruction is part of this task.

Restore validation first compares the shared logical oracle's schema and typed
row digests, then checks application invariants in the actual restored database.
Queue checks require valid states, bounded keys, nonnegative attempts, attempts
for claimed/completed jobs, exactly one receipt per completed job, and no orphan
or premature receipts. Cache checks require bounded keys, nonempty values, and
nonnegative expiry ticks. A matching but invalid source/restore pair fails.
Failures remain failed verification records through the existing reporting path.

Run the opt-in real file-replica smoke using the pinned binaries documented in
the logical verification guide:

```sh
GOTOOLCHAIN=go1.25.13 go test ./internal/worker -run TestChurnPinnedLitestream -v
```

Set SOAK_LOGICAL_LITESTREAM_BINARY and SOAK_LOGICAL_WORKLOAD_BINARY first. Without
them this test is explicitly skipped. It exercises both workload modes through
real replication, pinned restore, the worker validation pipeline, and corruption
rejection. It does not exercise S3 or Fly scheduling.
