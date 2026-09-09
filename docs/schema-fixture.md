# Schema migration and reclamation fixture

This local command is opt-in. It does not add workers or activate fleet profiles.
It creates a new private directory and refuses an existing directory, including
an empty one. Only generated fixture databases are mutated. All artifacts are
retained; choose a new directory for each attempt.

```sh
GOTOOLCHAIN=go1.25.13 go run ./cmd/schemafixture \
  -dir /tmp/schema-vacuum-run-001 \
  -litestream /absolute/path/to/pinned/litestream \
  -sha 4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3 \
  -rows 4096 -payload-bytes 1024 -vacuum vacuum
```

Use `-vacuum incremental` for incremental reclamation. Each variant performs
initialization, deterministic single-transaction growth, deletion of three
quarters of the rows, reuse with new IDs, index creation, column addition,
backfill, transactional table rebuild with a constraint and replacement index,
transactional schema/data rollback, and reclamation. Row counts must be multiples
of four. Increase `-rows` and `-payload-bytes` for large transactions; the default
is a small smoke fixture. The payload depends on ID and byte position, not time.

The writer is stopped at each boundary. Litestream's sync endpoint supplies a
replicated TXID and the restore explicitly requests that TXID. The shared logical
oracle compares schema, metadata and typed rows; restored application checks
also require row counts, BLOB payload lengths, and backfilled generations.
Recognized Litestream sequence bookkeeping follows the shared oracle policy.
There is no latest-restore fallback. A restore failure stops the scenario and is
retained even if Litestream could subsequently recover.

`boundaries.jsonl` is flushed after every attempt. `result.json` records options,
the supplied binary SHA (checked against its version), verdict and errors.
`replicate.log`, per-boundary restore logs, source and restored databases remain
available. Setup errors are inconclusive; operation/validation failures use the
shared rig failure verdict, and cancellation is aborted. No failed attempt is
retried into a successful result. Initialization waits for IPC readiness; that
startup wait is not a recovery test. Every sync attempt, including readiness
errors and lagging TXIDs, remains in the boundary evidence.

After the process exits, structured process evidence consumes the retained raw
log using patterns pinned to the tested Litestream SHA. ERROR/WARN records,
failure/retry messages and error fields retain line numbers, severity, messages
and aggregate counts. Up to 1,000 incident and unparsed-line details are retained
in JSON; raw logs remain complete, and truncation is explicit. Unknown log
formats, empty logs, read errors and unsupported binary versions never become a
clean pass. Startup socket-unavailable attempts (missing socket or connection refused before
the first successful sync) are labeled separately; the same errors after
initialization remain failures. A race-test attempt exposed the socket-created
but-not-listening interval and its failed output remains retained.

All verified boundaries plus process incidents yield `recovered_with_incidents`,
which exits nonzero and preserves all boundary proof. A partial/unavailable log
without recognized incidents yields `inconclusive`. Existing operation failures
remain failures. A pinned negative control injects an early ERROR record, then
performs all ten real restores successfully and requires the recovered verdict
and retained raw/structured incident. This is a classifier control, not a claim
that a real replication fault was injected or calibrated.

Boundary samples record database/WAL bytes, total local fixture file bytes
(including retained restores, logs and Litestream cache), local replica bytes, available disk
bytes, page/freelist counts, SQL operation duration and verification duration.
These are boundary samples, not peak resource measurements or concurrent request
latencies. Samples are captured after the paused boundary has synced and restored. Background
Litestream maintenance can still change object storage during a paginated listing;
these are completed-boundary observations, not atomic bucket snapshots.

File transport is the default and reports `remote_bytes: null`. For S3, supply
`-s3-endpoint`, `-s3-bucket`, and `-s3-environment emulator|provider`. Credentials
come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`; they are
never serialized in options or generated configuration. Each run generates a new
UUID prefix under `schema-fixture/` in an existing dedicated fixture bucket.
Remote measurements sum object sizes across all ListObjectsV2 pages for that
prefix; listing failures fail the boundary. Objects are retained. Local MinIO
results are explicitly labeled `s3-emulator` and do not establish provider behavior.

For a low-headroom variant, provision a dedicated disposable filesystem first,
then place the new fixture directory on it and pass `-max-headroom-bytes` with
the intended maximum available bytes. The command refuses to run if measured
headroom exceeds that limit, rather than treating an unengaged fixture as a pass.
It never mounts, fills, truncates or removes external data to manufacture pressure.
Set the temporary-directory environment appropriately when evaluating VACUUM's
temporary storage on that filesystem. A small max-page-count is not a substitute
for actual disk headroom. Disk exhaustion is retained as a failure, not accepted
as successful recovery. The ordinary smoke does not establish constrained-disk
coverage; record that variant separately when provisioned.

The bounded Docker runner executes both an actual control and a disk-full case:

```sh
SCHEMA_FIXTURE_IMAGE=your-local-pinned-worker-image scripts/schema-fixture-disk.sh control
SCHEMA_FIXTURE_IMAGE=your-local-pinned-worker-image scripts/schema-fixture-disk.sh negative
```

The image must contain `/usr/local/bin/litestream` with the expected pinned SHA
and `/bin/sh`. The script builds the fixture command for the image architecture,
disables container networking, limits memory/CPU, and uses a 256 MiB control or
16 MiB negative tmpfs for source, replicas, restores and SQLite temporary files.
It copies generated evidence to a new local artifact directory before exiting.
The host-side `output.json` is authoritative when the fixture filesystem is
full: it retains the operation error and any failure to write `result.json` or
boundary evidence. An empty/truncated in-fixture result is retained as failure
evidence, never interpreted as success. Stopped containers are retained. The negative command succeeds only when it
observes a disk-full failure; the scenario result itself remains a failure.
No host filler files, mounts or production data are used.

SQLite documents that [VACUUM](https://sqlite.org/lang_vacuum.html) may need up to
twice the database size in free space, and describes the transactional rebuild
procedure under [ALTER TABLE](https://sqlite.org/lang_altertable.html).

Pinned real restore tests:

```sh
SOAK_SCHEMA_LITESTREAM_BINARY=/absolute/path/to/pinned/litestream \
  GOTOOLCHAIN=go1.25.13 go test ./internal/worker -run TestSchemaFixture -v
```

Set `SOAK_SCHEMA_S3_ENDPOINT` plus the AWS fixture credentials to additionally
exercise both variants against the `schema220` test bucket on isolated MinIO.
Without those variables, the corresponding real transport tests explicitly skip;
deterministic SQL transition, rollback, corruption and fixture-safety tests still
run. No remote or low-headroom success should be inferred from those unit tests.

## Executed development evidence

Pinned SHA `4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3` was exercised with
both vacuum modes on file replicas and isolated MinIO. All ten boundaries in
each variant restored and matched the shared oracle. A separate MinIO run
committed 16,384 rows of 4,096 bytes (64 MiB payload) in one transaction and
completed all boundaries. The bounded-disk runner completed the 256 MiB control
and preserved SQLite disk-full failure on the 16 MiB negative fixture. These
are emulator/local filesystem results, not production provider certification.

For a dedicated local emulator, start MinIO with a bounded data tmpfs and loopback
port, create a dedicated test bucket, and pass that endpoint to the command.
Do not point the fixture at a production bucket. The command and disk runner
retain their resources and artifacts for review; cleanup is a separate action.

One repeated 64 MiB MinIO run under concurrent local validation failed at growth
with a sync HTTP 500 deadline error when individual sync requests had a one-second
budget. That attempt and its raw output remain failure evidence. Sync requests
now use the existing 30-second boundary budget; failures still stop the run and
are never retried into success. Successful runs do not erase this observation.
