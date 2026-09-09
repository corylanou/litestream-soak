# Reproducible local comparisons

`soakcompare` is an opt-in local experiment runner. It does not activate Fly
workers, merge sources, or change fleet settings. Existing deployment scorecards
remain observational comparisons; they are not controlled benchmark results.

The runner executes real Litestream binaries, fresh SQLite fixture copies,
operation-budgeted SQL writes, file or S3 replication, graceful shutdown, restore,
and the shared logical oracle for schema, application metadata and rows. Tests with in-memory observations exercise
report logic only and are not benchmark evidence.

## Prepare immutable inputs

Build the harness from one recorded source revision using Go 1.25.13. Use that
same harness binary for the entire experiment. Verify `go version -m` reports
`vcs.revision` and `vcs.modified=false`. If the toolchain does not stamp a linked
worktree, build the exact revision from a clean standalone clone; do not bypass
the execution provenance check. Its generator and oracle are
independent of the candidate Litestream source. Neither uses `litestream-test`
from a candidate build.

```sh
GOTOOLCHAIN=go1.25.13 go build -buildvcs=true -o bin/soakcompare ./cmd/soakcompare
bin/soakcompare -fixture /absolute/path/fixture.db -seed 42 -rows 1000
```

Fixture creation refuses existing files. Keep the fixture read-only during the
experiment. Its bytes, size, seed and age **at capture** must be recorded in the
plan; age describes the frozen fixture's workload history, not elapsed time
between successive executions. This initial generator creates an unaged fixture
(age zero). An aged fixture must preserve the `fixture` and empty `operations`
tables and comes from separately recorded preparation; this command does not
simulate aging by changing timestamps.

Inspect the fixture to obtain its measured digest/size, local hardware identity,
compiler, and runtime configuration digest:

```sh
bin/soakcompare -inspect /absolute/path/fixture.db -seed 42 > contract.json
```

Inspection reports the build source revision only when metadata identifies a
clean build. Dirty or unavailable source metadata leaves source identities empty. Set the operation budget and fixture age explicitly. Create a plan
with these fields (replace placeholders with measured values):

```json
{
  "id": "local-comparison-unique-id",
  "baseline": "latest-release",
  "candidate": "branch:my-fix",
  "repeats": 4,
  "contract": {
    "backend": "file",
    "fixture_sha256": "FULL_SHA256_OF_FIXTURE",
    "fixture_bytes": 24576,
    "fixture_age_seconds": 0,
    "seed": 42,
    "generator_sha": "FULL_HARNESS_SOURCE_COMMIT",
    "oracle_sha": "FULL_HARNESS_SOURCE_COMMIT",
    "hardware": "HOSTNAME/OS/ARCH/cpus=COUNT",
    "region": "local",
    "config_sha256": "LOCAL_CONFIG_DIGEST",
    "operation_budget": 1000,
    "toolchain": "go1.25.13"
  }
}
```

The local hardware identity is the hostname, Go OS/architecture and logical CPU
count. Local execution checks it and the harness/compiler version. Record RAM,
CPU model, power mode and competing host activity alongside the experiment when
interpreting results; the runner cannot reserve a host or eliminate external
load. Each pair runs sequentially on that same host. `region` must be `local`;
remote regional experiments require a separate executor, not a relabeled local
run. `LocalConfigSHA256()` identifies the fixed configuration and scenario
parameters below.

Resolve moving references once, before building any candidate binaries:

```sh
bin/soakcompare -plan requested.json -pin > pinned.json
```

Pinning uses `gh api` against `benbjohnson/litestream`, resolves `main`,
`latest-release`, `branch:NAME` (or a plain branch/tag) to full commit SHAs, and
reuses a resolved value for repeated references. `main_sha` is pinned separately
for calibration. Execution accepts only full SHAs and never resolves them again.
Build each distinct pinned SHA with the same compiler and embed the complete SHA
in `main.Version`, as the existing local rig does. The runner checks the binary
version and Go compiler metadata and retains its SHA-256 and version output.
Do not build from a moving branch after pinning.

Create a local executor configuration with absolute paths:

```json
{
  "fixture": "/absolute/path/fixture.db",
  "directory": "/absolute/path/retained-experiments",
  "harness_sha": "FULL_HARNESS_SOURCE_COMMIT",
  "binaries": {
    "FULL_PINNED_MAIN_SHA": "/absolute/path/main-litestream",
    "FULL_PINNED_BASELINE_SHA": "/absolute/path/baseline-litestream",
    "FULL_PINNED_CANDIDATE_SHA": "/absolute/path/candidate-litestream"
  },
  "timeout": 60000000000
}
```

`timeout` is a positive per-execution duration in nanoseconds (the example is
60 seconds). All configuration fields are checked; unknown JSON fields and
trailing documents are rejected. CLI execution requires clean Go build metadata and rejects a missing, dirty or
mismatched VCS revision before creating runs. The declared harness source SHA must
equal that revision. Each execution archives the actual harness binary SHA-256, compiler and available
VCS metadata as well as the declared source revision. Archive compiler
build information with the report. This checks embedded metadata; it is not a signed supply-chain attestation.

```sh
bin/soakcompare -plan pinned.json -local local.json > report.json
```

Use a new experiment ID for every invocation. Existing run directories are
rejected, never overwritten. Retain stdout even on a nonzero exit: failures do
not suppress subsequent pairs. Cancellation stops new executions and returns
partial evidence. Each started run also retains its request, observation,
source, replica, restored database, configuration and process logs. Preserve
these artifacts when retrying; a later clean execution does not erase an earlier
failure. This local evidence does not claim integration with the durable fleet
incident ledger from #211.

## Executed matrix and controls

Every experiment executes `16 × repeats` runs: calibration and comparison, four
scenarios, and both arms. Two repeats is the minimum; 100 is the maximum.
Calibration executes pinned main versus the same pinned main. Comparison executes
baseline versus candidate. Pair zero is baseline-first, pair one candidate-first,
and ordering continues alternating. Each arm gets the same immutable fixture,
seed, harness, configuration and completed-operation budget. Replica directories
are isolated by experiment, kind, scenario, pair and arm.

| Scenario | Payload per committed operation | Pacing |
|---|---:|---|
| Favorable | 128 bytes | 1 ms after each operation |
| Representative | 1,024 bytes | 1 ms after each operation |
| Saturation pressure | 16,384 bytes | Unpaced single writer |
| No Litestream | 1,024 bytes | 1 ms; no replica process |

These are explicit workload controls, not claims that any particular machine
was saturated or that the representative workload matches all production use.
Each operation is one committed insert. SQLite uses WAL, disabled automatic
checkpointing, and a 5-second busy timeout. Replication sync/checkpoint intervals
are 100 ms. Startup probes the Unix socket for up to one second. The workload ends with
confirmed bounded sync requests, progress sampling and allocation capture before
shutdown. Shutdown and restore must complete inside the run timeout. Incomplete budgets and failed
restores are retained as adverse evidence, never normalized into successful
measurements. The shared oracle compares source and restore schema, `user_version`,
`application_id`, types and data. Independent deterministic checks also verify
every expected operation ID/value and the immutable fixture, including missing,
changed and extra rows.

## Evidence and interpretation

All metric summaries keep their own verdict. CPU improvement cannot cancel
correctness failures, incidents, RSS regressions, or another adverse category.
The overall verdict is `adverse` if any such evidence exists,
`no_adverse_observed` when every scheduled execution completes without adverse
evidence, and `inconclusive` for partial execution. This is not a clean-soak
verdict. Correctness and performance have separate verdicts; noisy performance
remains inconclusive and unavailable capabilities remain explicit.

| Field | Measured scope or explicit limitation |
|---|---|
| Correctness | Exact restored fixture and operation rows; local control checks source rows |
| Reliability | Unavailable as complete-run eligibility; process failures and all warning/error log lines retained, including recovered warnings |
| CPU/operation | Candidate replication process user + system CPU, including startup/drain/shutdown, divided by completed operations |
| Allocation/operation | Candidate runtime TotalAlloc delta from retained pprof heap text before writes and after sync, divided by completed operations; includes observation overhead |
| RSS | Candidate process lifetime peak resident bytes from OS resource usage |
| Latency p50/p95/p99 | Nearest-rank SQL-operation wall-time percentiles, excluding deliberate pacing |
| Lag | Maximum observed pending-TXID age where read-only source/replica TXIDs exist; explicit unavailable capability otherwise |
| Replica sync age | Separately measured age since `last_sync_at`; not transaction lag |
| Restore time | Wall time of actual restore subprocess |
| Disk growth | Source database/WAL/shared-memory, Litestream staging, and local replica bytes, excluding restored copy, configuration and evidence logs |
| FD growth | Linux `/proc` end minus baseline, with baseline/peak/end and process identity retained; unsupported on non-Linux hosts |
| Replica bytes | Retained file-replica size; S3 retained size unavailable without a bucket inventory |
| Transfer bytes | S3 request/response body bytes including retries and restore; distinct from retained size |
| Object requests | S3: actual proxied HTTP attempts across replication and restore, including failed requests/retries; unavailable for file replicas |

Candidate-only measurements in the no-Litestream control are explicitly
unavailable. Unsupported CLI/configuration in a release is an execution failure,
not a skipped pass. Log scanning is conservative and retains matching raw lines;
it is not a complete structured incident detector. No maintenance exposure, restart reliability, or regional fleet result is inferred.

Each metric includes paired sample count, candidate-minus-baseline mean delta,
a two-sided Student-t 95% interval, and the maximum absolute main/main paired
delta as a calibration noise floor. All pairs and calibration measurements must
be present for a directional verdict. An interval wholly above the noise floor
is a regression; wholly below its negative is an improvement. Otherwise it is
inconclusive. Lower is better for all reported quantities. The interval assumes
independent paired differences; temporal dependence, multiple comparisons and
host contention can invalidate inference. Calibration is reported separately,
not subtracted to manufacture an improvement. Raw observations remain available
for further analysis. Missing measurements carry reasons and `unavailable`
summaries, not numeric zero samples.

## Validation

The ordinary test suite executes the real SQL no-Litestream control and negative
oracle tests. To execute the full matrix against an explicitly supplied real
binary (same SHA on both arms for execution validation):

```sh
GOTOOLCHAIN=go1.25.13 \
SOAK_COMPARE_TEST_BINARY=/absolute/path/litestream \
SOAK_COMPARE_TEST_SHA=FULL_BINARY_SHA \
SOAK_COMPARE_TEST_BUDGET=1000 \
SOAK_COMPARE_TEST_REPORT=/absolute/path/new-report.json \
go test ./internal/compare -run TestLocalRealBinary -count=1 -v
```

This opt-in integration test executes 32 runs. Its small fixture and operation
budget validate the runner, not performance claims. Benchmark campaigns need
larger predeclared budgets, repeats, isolated hardware, retained artifacts and
review of every unavailable capability.

## S3 comparisons

Set contract `backend` to `s3` and pin `replica_endpoint`, `replica_bucket`, and
`replica_region`. The bucket must already exist. Credentials come only from
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN`;
never put credentials in the plan or endpoint. Every run uses its isolated
experiment prefix. Objects are retained; the runner does not clean the bucket.

A loopback observer re-signs requests using the existing S3 signing transport,
counts each HTTP attempt and actual request/response body bytes, and records all
HTTP failures even if the client retries successfully. These counts cover
replication and restore, exclude HTTP headers and TLS framing, and include
observer overhead. Keep the same backend and observer configuration for both
arms. Network/provider variance remains subject to calibration. Failed restores
still retain the observer evidence. Local MinIO execution validates this path;
it does not establish performance on a remote S3 service.

The instrumented runner uses Linux `/proc` and candidate Unix-socket pprof/progress
APIs where available. A binary without a required endpoint reports the concrete
HTTP or capability failure. A process/IPC collection gap is retained as an
incident; old samples are never substituted for a fresh observation. Profiling
and polling add workload overhead, which is matched across the pair and should
be included in interpretation. The no-Litestream control has no candidate
allocation, FD, replication lag, restore or object-request measurements.

Progress sampling is read-only. It uses `/list` and, when needed,
`GET /debug/sync-status?path=...`, retaining the actual nested `databases[]`
diagnostic. The pinned 4ed7 binary exposes local TXID but not replicated TXID in
these diagnostics, so transaction-lag seconds remain explicitly unavailable.
Its `last_sync_at` supports the separate `replica_sync_age_seconds` metric,
which is age since a successful replica sync, not transaction lag. No sampler
issues POST requests. POST `/sync` is reserved for the declared end boundary.

Earlier development runs using sync probes are retained as observer-intervention
evidence and are not natural-replication benchmarks. The configuration digest
changes with the observation policy; reports using different policies are not
matched comparisons.

## Retained development validation scope

The committed harness `211975ccffcd5512d64c2df5307df750f77454da` executed a
32-run main/main file campaign against clean Litestream
`4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3`, with a new 1,000-row fixture,
declared age zero, two pairs per scenario and 1,000 committed operations per run.
All 32 logical checks passed. One saturation-pressure baseline run logged
`compaction failed` with `read database page 306: invalid argument`; the report
retained it and returned `adverse`. Performance remained `inconclusive`.
The unresolved measured incident is tracked in
[#255](https://github.com/corylanou/litestream-soak/issues/255); no root cause or
fix is established. The tenant lifecycle campaign saw a similar error, which is
a related observation rather than evidence of a common cause.
This is bounded validation on a shared macOS host, not a candidate performance
claim or proof of saturation. Requests, observations, profiles, source/restore
copies and logs remain retained with the campaign.

Earlier Linux/file and macOS/MinIO 32-run test campaigns used placeholder harness
identities and a synthetic test fixture age. They demonstrate real collector
execution, including Linux FD samples and S3 attempts/transfer bytes, but do not
establish immutable harness provenance or measured fixture aging. Earlier failed
runs and mutating sync-probe runs remain retained with their original outcomes.
