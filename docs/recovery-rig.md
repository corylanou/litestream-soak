# Local recovery comparison

`soakrecovery` is an opt-in, bounded local file-replica experiment. Run the same
command separately for each baseline/candidate executable, with a fresh output
directory and identical window. It never activates a fleet or touches a supplied
database. Process SIGKILL tests process interruption, **not power loss**.

Build a Litestream executable from an immutable source revision, record that
revision and compiler, and supply its independently calculated SHA-256:

```sh
GOTOOLCHAIN=go1.25.13 go run ./cmd/soakrecovery \
  -binary /absolute/path/to/litestream \
  -sha256 <64-character-sha256> \
  -output /absolute/path/to/new-evidence-directory \
  -window 30s
```

The directory must not exist. The runner creates a roughly 32 MiB append-only
SQLite fixture and retains restored databases, quarantined original state,
replica objects, process logs, and `result.json`. Allow several GiB for retained
artifacts. Nothing is automatically removed. The command has a three-minute
outer bound; a scenario that misses engagement remains inconclusive and exits
nonzero. Existing output directories and incorrect pins are rejected.

The result includes the executable hash, reported version, and literal restore
help for that executable. Timestamp and follow checks are attempted when the pin
advertises them; absent flags produce explicit unexecuted checks, never passes.
Follow resume is verified by reopening the same output and saved `-txid` sidecar,
observing the resume log, and validating a new commit. Pins with different
configuration or log contracts fail or remain inconclusive; do not infer support
for untested refs from another ref's result. The exercised pin is
`4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3`.

The controls execute:

- SIGKILL of the replicator, verified through its child wait status; commits
  while it is offline; restore of the last available replica; restart and restore.
- SIGKILL after a restore has produced nonempty output, followed by recovery to a
  separate output. A restore that finishes before the kill does not engage.
- Follow interruption and resume with its existing database and progress sidecar.
- Repeated restores while a writer commits and Litestream performs maintenance.
  Exposure requires actual L0 opens and positive retention deletion plus compaction
  during that restore process, excluding subsequent oracle validation.
- A timestamp target before later commits, followed by that same target after
  retention. A retained target must match the original prefix; an expired target
  must return the pin's explicit unavailable-backup error. Latest recovery is
  validated separately so expected retention never excuses replicated data loss.
- Local-loss recovery after moving the database, sidecars and Litestream cache
  into a quarantine directory. Only the replica is available at the configured
  source location; the quarantined database remains the independent oracle.

Committed rows and confirmed replicated rows are separate boundaries. Confirmation
requires an actual restore and logical validation, never a successful commit,
process restart, elapsed delay, or object count. Each attempt reports unconfirmed
backlog, restored rows, asynchronous tail loss, and loss below the confirmed
boundary separately. These are row windows, not an estimate of time-based RPO.
Historical and expired targets carry separate boundary scopes; intentionally
excluded later commits are not reported as asynchronous loss.
The oracle checks integrity, every ID/value/storage type, and complete append-only
transaction prefixes using the rig oracle established by #215. It complements
#204's general workload oracle; it does not claim coverage of arbitrary schemas.

Every attempt retains its error, duration, log and exposure proof. Expected kills
and unavailable expired targets retain their nonzero errors. Earlier unexpected
errors, warnings, retries and self-healing messages prevent a passing verdict.
The pin's debug-only startup readiness scheduling is retained explicitly as an
observation. Successful later attempts do not erase previous failures. Attempts
without maintenance exposure remain individual observations; a separate exposure
check requires at least one valid exposed restore.

Run the bounded integration test explicitly:

```sh
SOAK_RECOVERY_BINARY=/absolute/path/to/litestream \
SOAK_RECOVERY_SHA256=<64-character-sha256> \
SOAK_RECOVERY_OUTPUT=/absolute/path/to/new-test-evidence \
GOTOOLCHAIN=go1.25.13 go test ./internal/recovery -run TestRecoveryPinnedBinary -v -count=1
```

Without the binary environment variable this expensive test is skipped, visibly.
Unit tests still cover corruption, boundary accounting, missing exposure, retained
failures, capability detection, fixture protection and process cancellation.

This harness owns fresh isolated state. #217's persistent-upgrade fixture owns
aged-state imports and transitions; run both experiments for an upgrade comparison.
Neither local experiment establishes provider correctness, power-loss durability,
fleet behavior, or the upstream #107 Fly A/B calibration.
