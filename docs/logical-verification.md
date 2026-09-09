# Logical restore verification

Every worker verification now requires an independent logical match in addition
to SQLite structural integrity. The oracle is `soak-logical-v1`, implemented by
the soak harness and its pinned SQLite dependency, not the candidate Litestream
validator. The candidate still performs the restore. `quick`, `integrity`,
`checksum`, and `full` requests all use integrity plus logical comparison;
physical file checksums are no longer a correctness gate.

## Restore boundary

The worker pauses configured load producers, checkpoints, and waits for
replication. After any checkpoint thaw/re-pause, it acquires SQLite's writer
reservation (`BEGIN IMMEDIATE`) before a second sync and source snapshot.
An in-flight writer prevents acquisition; sending SIGSTOP alone is not treated
as proof of quiescence. The reserved sync must report the requested source TXID
and replication through that TXID. The replica watermark is not used as the
source boundary. Schema and all tables are read in one consistent transaction
while the reservation excludes commits, including writers outside the harness.
The reservation is released before restoring; load producers resume on every
cycle exit, including cancellation.

Restore invokes `litestream restore -txid` directly, followed by independent
SQLite integrity and logical/schema comparison. The shipped 4ed7a308 binary
supports this path even though the independently pinned workload helper does
not accept `validate -txid`. There is no latest fallback. Missing pin support,
zero TXIDs, unavailable writer reservations, sync errors, or boundary drift
produce failed attempts with explicit unavailable evidence and no correctness
credit. A later cycle acquires a fresh boundary; it does not replace the earlier
failed attempt. The immutable source digest remains unchanged during restore,
so a newer replica commit cannot change a successful pinned comparison.

Evidence separates workload/run identity from `boundary_txid`,
`source_boundary=writer-reserved-sync`, and `restore_boundary=pinned`.
The historical Amsterdam 126454/126455 mismatch remains a failure with an
unproven cause; its unpinned restore does not establish Litestream corruption.

Many-database verification applies the same oracle separately to each selected
database and releases each restored database after comparison. A truncated
changed-database set is an explicit failed/incomplete check, not a passing fleet
result. Raise `VERIFY_CHANGED_LIMIT` to cover the changed set. A failure remains
in the append-only verification log and reported steps even if a later cycle
passes.

## Equality contract

The oracle compares stored schema definitions for tables, indexes, views, and
triggers, plus `user_version` and `application_id`. SQL definition text is compared conservatively; semantically equivalent
DDL written differently is not normalized. It includes SQLite's sequence and
statistics tables, so ordinary `ANALYZE` databases are supported.

Rows are hashed with type tags and byte lengths. NULL, signed 64-bit integers,
IEEE real values, TEXT bytes (including embedded NUL), and BLOB bytes remain
distinct. Duplicate multiplicity matters. Accessible implicit rowids are included;
a table with all three aliases (`rowid`, `_rowid_`, `oid`) shadowed has no
SQL-accessible implicit rowid to compare. Rowid tables stream in rowid order;
`WITHOUT ROWID` tables stream in primary-key order. Tables with every rowid alias
shadowed use a typed, binary-collated visible-column order. This avoids a
payload-wide sort for normal fleet databases. Physical page layout, free pages,
and WAL checkpoint placement do not affect equality.

There is one narrowly defined bookkeeping exception: the contents of
`_litestream_seq` with the exact upstream schema
`CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER)` are excluded.
Its schema is still compared, and its rows must be empty or the single integer
sequence row with `id=1` and a positive integer `seq`. Litestream can advance
this counter without emitting a new replica transaction. `_litestream_lock`
remains compared, including unexpected committed rows. Other names, including
`_litestream_seq_user`, and a different schema at `_litestream_seq` receive no
exception. Every logical step names this policy in its evidence.

Virtual/shadow tables and over-wide or oversized objects fail explicitly; they
need module-specific validation. No application table is excluded by a name
prefix. Views and triggers are compared as schema, not executed. Digest equality
uses SHA-256 and consequently carries its usual negligible collision risk.

## Budgets and evidence

| Environment variable | Default | Scope |
| --- | --- | --- |
| `VERIFY_LOGICAL_MAX_ROWS` | 1,000,000,000 | Rows per database snapshot |
| `VERIFY_LOGICAL_MAX_BYTES` | 1,099,511,627,776 (1 TiB) | Encoded schema and row bytes per snapshot |
| `VERIFY_LOGICAL_MAX_OBJECTS` | 4,096 | Stored schema objects per snapshot |
| `VERIFY_LOGICAL_MAX_VALUE_BYTES` | 67,108,864 (64 MiB) | SQLite value/row length limit |
| `VERIFY_LOGICAL_TIMEOUT` | `30m` | Source scan, restore, and comparison per database |

All overrides must be positive. Column count is limited to 256 and object/column
names to 1,024 bytes. SQLite uses a 2 MiB page cache, file-backed temporary
storage, and no sorter worker threads. The Go comparator retains one row and
bounded schema/table metadata, not the database's rows or their hashes. A
fallback sort may consume temporary disk proportional to its table; disk errors
and deadline/size exhaustion fail the check. Large databases can still require
substantial sequential I/O and a long load pause. These defaults accommodate
existing 100 GB fleet volumes without a small fixed row/byte cap; operators can
adjust the explicit budgets for larger workloads. This change does not activate
new fleet scenarios.

Mismatch evidence includes the database, requested TXID, validator and workload
identities, table name, row counts, and source/restored SHA-256 digests. Schema
mismatches retain both schema digests. Row values are not logged. External tool
output is capped at 64 KiB and labeled if truncated. Unsupported objects and
budget failures carry explicit errors rather than `logical_match=true`.

## Independent workload build

The worker image builds `litestream-test` from the independent `WORKLOAD_SHA`
build argument, defaulting to upstream v0.5.10 commit
`ae88b164dd6304bcbb654a681df767ee59042eed`. Changing `LITESTREAM_SHA` changes the
candidate without changing this workload pin. The runtime `WORKLOAD_SHA` and
soak SHA appear alongside `soak-logical-v1` in verification evidence; local
unattributed tools are explicitly `unknown`. Deployment identity attribution is
handled by the reporting workflow separately.

All three worker builder stages retain Go 1.25.13, `GOTOOLCHAIN=local`, and binary
compiler metadata. Integration of these version strings into immutable run
identity belongs to audit issue #205.

## Opt-in real restore test

`TestLogicalOraclePinnedLitestream` requires a binary built from
`4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3`, with that SHA as `main.Version`.
Set `SOAK_LOGICAL_LITESTREAM_BINARY` to its absolute path and
`SOAK_LOGICAL_WORKLOAD_BINARY` to `litestream-test` built from
`ae88b164dd6304bcbb654a681df767ee59042eed`, with that SHA as `main.Version`. Run:

```sh
GOTOOLCHAIN=go1.25.13 go test ./internal/worker -run TestLogicalOraclePinnedLitestream -count=1 -v
```

The test starts a temporary file replica, commits byte-sensitive rows in WAL,
forces checkpoint bookkeeping, restores a pinned TXID, and exercises the actual
`validateDB` pipeline with both real binaries, including its labeled TXID fallback
and restored-path handling. It verifies logical equality,
advances the bookkeeping sequence, and proves deletion of a committed row fails.
It does not use production credentials or change fleet configuration.
