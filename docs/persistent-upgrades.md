# Local persistent upgrade comparisons

`soakupgrade` is an opt-in local rig. It does not change fleet replacement,
activate a profile, call Fly or object-storage APIs, or delete artifacts. Normal
fleet replacement continues to use fresh state. Run this command manually with
trusted binaries and a new output directory:

```sh
GOTOOLCHAIN=go1.25.13 go run ./cmd/soakupgrade \
  -mode persistent-upgrade \
  -baseline /absolute/path/to/baseline-litestream \
  -baseline-sha256 BASELINE_BINARY_SHA256 \
  -candidate /absolute/path/to/candidate-litestream \
  -candidate-sha256 CANDIDATE_BINARY_SHA256 \
  -transition ltx-v1 -rollback-supported \
  -output /absolute/path/to/new-comparison
```

Obtain the expected binary SHA-256 digests from your trusted build artifacts;
record their source revisions and compiler metadata with the experiment.
The result records the binary Go version, module version, source revision, and
dirty-build marker when present in Go build metadata. The runner verifies the
supplied binary bytes before starting and before every subsequent process invocation. It never builds
or modifies a candidate dependency graph. A binary digest identifies the actual
executable even when `litestream version` reports a development build.

## Modes and isolation

- `fresh-start` (default) seeds independent small databases and new replica
  histories with each arm's own binary from its first start before comparison. It never imports old
  state and refuses `-fixture`.
- `persistent-upgrade` creates a baseline fixture and drives updates, deletes,
  and replacement inserts until the fixture has at least two completed
  snapshots, two completed compactions, positive L0 retention deletion counts,
  and committed updates and deletes. `-age-timeout` bounds this attempt; elapsed
  time alone never earns an aged verdict. The default is one minute.

The rig owns and stops its database writer and Litestream process before
copying. It closes the writer, allows synchronization, and requires clean
replicator shutdown. The entire state directory is copied, including the SQLite
database, sidecars, Litestream metadata, and every replica object. The original
fixture, baseline arm, candidate arm, and rollback arm have distinct database
paths and replica directories. Copies contain independent file bytes, with no
hard links. Existing destinations, overlapping trees, symlinks, and special
files are rejected.

The file replica backend represents remote history in this local experiment:
its complete object tree is copied, not a newly restored database or a fresh
replica. This validates persisted history and metadata compatibility without
live S3 credentials. It does not validate provider-specific copying, S3 version
history, multipart operations, or fleet volume migration. Those remain
unexecuted and require a separately activated provider experiment.

After quiescence, a fixture seal records every file's SHA-256, its observed age,
the baseline binary digest, and any fixture failure. The sealed contents include
the maintenance log. Reuse a retained fixture with:

```sh
GOTOOLCHAIN=go1.25.13 go run ./cmd/soakupgrade \
  -mode persistent-upgrade -fixture /absolute/path/to/previous-comparison/fixture \
  -baseline /absolute/path/to/baseline-litestream \
  -baseline-sha256 BASELINE_BINARY_SHA256 \
  -candidate /absolute/path/to/candidate-litestream \
  -candidate-sha256 CANDIDATE_BINARY_SHA256 \
  -transition ltx-v1 -rollback-supported \
  -output /absolute/path/to/new-comparison
```

Reuse requires the same baseline pin and verifies the copied bytes against the
seal before starting either arm. Keep the fixture and its adjacent JSON seal
together. The seal detects accidental mutation; it is not an authentication
boundary against someone who can rewrite the artifact and seal. Never point
this command at an active database or hand-author a seal to bypass quiescence.
A retained fixture failure remains a failure on reuse, and the failed copy is
retained for inspection.

## Checks and evidence

Both arms begin from the same aged state in persistent mode. Before continuation,
each binary restores its arm. Baseline and candidate then perform the same
number of deterministic churn transactions, derived from `-continue-for`
(default five seconds, one transaction every 200ms after observed replicator readiness). Each stopped arm is restored
again. Supported rollback runs the baseline binary against a separate copy of
the candidate's post-upgrade database, metadata, and replica history, then
restores that copy. The candidate's evidence is retained intact.

Every restore must pass SQLite integrity checking and the shared `soak-logical-v1`
oracle used by workers: schema, application metadata (`user_version` and
`application_id`), typed row contents, and the recognized Litestream bookkeeping
policy must match the quiescent source. Resource limits and unsupported-schema
behavior are shared with worker verification. The generated workload is small
deterministic churn, not a long-duration capacity benchmark.

`result.json` records binary pins, mode, age observations, and every check as
`passed`, `failed`, `unsupported`, `unexecuted`, or `inconclusive`. Logs, failed
restore outputs, original fixture, both comparison arms, and rollback state are
retained. A failed pre-upgrade check or replication warning/error/retry/self-heal log cannot be erased
by a later successful restore. Missing maintenance exposure and omitted rollback
support produce an inconclusive overall result, never a pass. Interrupted runs
retain failure and unexecuted checks. A non-passing overall result exits nonzero.
There is no automatic cleanup; archive known-bad evidence before any separately
authorized cleanup.

## Supported transitions

`-transition ltx-v1` explicitly declares that both pinned binaries use compatible
v0.5-era LTX storage and the generated configuration. It is an experiment input,
not an inferred compatibility guarantee. `-rollback-supported` additionally
asserts that running the baseline against candidate-written state is a supported
transition worth testing. Unknown formats, v0.3/WAL migrations, encrypted
migration paths, and undeclared rollback are explicitly unsupported. They are
never silently treated as passing checks. Consult Litestream's
[migration guide](https://litestream.io/docs/migration/) when selecting pins.
Actual restore and continuation checks must still succeed for a declared pair.

## Validation

Unit tests use a controlled subprocess to check ordering, isolation, artifact
retention, unsupported transitions, cancellation, and failure persistence.
They do not establish real Litestream compatibility. To run both lifecycle
modes against an actual local binary (including rollback to itself):

```sh
UPGRADE_TEST_BINARY=/absolute/path/to/litestream \
  GOTOOLCHAIN=go1.25.13 go test ./internal/upgrade \
  -run TestPinnedBinaryLifecycle -count=1 -v
```

Use the CLI with distinct pinned binaries for a retained version comparison.
No successful local run authorizes fleet activation or deployment.
