# Opt-in tenant lifecycle fixture

`tenantfixture` exercises directory replication without enabling any Fly workers
or changing the normal many-database profiles. Run each binary and tier separately;
a two-tenant smoke is not resource-scaling evidence for 100, 500, or 1000 tenants.
The fixture currently uses local file replicas, not S3. Its generated configuration
uses a 100ms checkpoint interval, a one-page minimum checkpoint threshold, and
a 100ms replica sync interval to exercise discovery promptly. These maintenance
settings are preserved with every run and differ from normal fleet profiles;
resource comparisons require the same fixture configuration and storage substrate.

```sh
GOTOOLCHAIN=go1.25.13 go build -o bin/tenantfixture ./cmd/tenantfixture
mkdir -p .local-rig/tenants
bin/tenantfixture \
  -litestream /absolute/path/to/litestream \
  -sha 4ed7a308f6271ebfd2b0a6e4b70b03011a37e4a3 \
  -capabilities directory-v1 \
  -output .local-rig/tenants -mode watch -tenants 100 \
  -timeout 30s -sync-timeout 1s
```

Repeat with `-mode static` and the existing tier sizes `100`, `500`, and `1000`.
For main or a candidate, build the desired immutable commit with its full SHA as
`main.Version`, and pass that SHA to `-sha`. Moving refs and version mismatches
are rejected. The explicit `directory-v1` contract asserts that the selected
binary supports directory configuration, `watch`, relative-name replica prefixes,
IPC `/list` and `/sync`, and direct file-URL restores. Unknown contracts are
rejected; candidate hashes are not restricted to an allowlist. The reference
contract was smoke-tested against the SHA above. An incompatible candidate fails
with retained evidence; it is never silently credited with unexecuted checks.

The [upstream directory watcher guide](https://litestream.io/guides/directory-watcher/)
describes the contract. Static scans must leave runtime creations undiscovered
until restart. The fixture observes absence for one second before restarting.
Watcher mode must discover creations and remove retired paths without restart.
Static removal is performed while stopped, then checked after restart.

The scenario seeds one fewer than the selected tier, starts replication, restores
every initial tenant, then creates the final tenant at runtime. It writes ten
512-byte rows to the hot tenant and one row to every cold tenant. Every tenant is
then synced and independently restored with the existing bounded logical oracle.
The hot tenant is retired by moving its source out of the discovery pattern;
its retained backup must still match its saved logical snapshot. Recreation uses
a new generation name and replica prefix with a different identity row. A final
restart must rediscover all live tenants, omit the retired generation, and recover
both live and retained backups independently.

Every run owns a fresh private directory. No existing databases, configuration,
replica prefixes, or environment files are reused. Generation names isolate both
metadata and replicas. The fixture does not test overwriting a retired tenant's
same physical filename/prefix. Writes and lifecycle transitions are serialized;
acknowledgements name the exact generation. Failed work remains pending, attempts
rotate behind unattempted peers, and a batch checks all peers even after a failure.
A failing batch stops the scenario and returns nonzero without erasing failures.

`report.json` and stdout contain the verdict, per-tenant phase results, polling
attempts including errors, capability/pin identity, and resource frames. Full
replication and restore output is retained in `process.log`; the report also has
a small diagnostic tail. WARN, ERROR, retry/recovery messages, and error attributes remain incidents even
after later success. Per-line evidence records severity, message, raw excerpt,
and line number; the complete raw log remains authoritative. The `slog-text-v1`
parser contract is reported separately from the binary SHA. Empty logs, unknown
formats, malformed lines, scanner errors, or truncated incident summaries prevent
a clean pass. Summaries retain at most 1000 incidents while exact incident and
unparsed counts continue to increase; raw output is retained up to its log budget.
Readiness polling can legitimately record IPC-not-ready errors before the socket
exists; these are retained as pending attempts, not reclassified as successful
attempts. Each sync/discovery attempt is tied to its tenant and phase. Recovered
non-startup IPC errors remain failures in the final verdict.

The operation deadline (`-timeout`, default 30s) is distinct from the sync request
deadline (`-sync-timeout`, default 1s). The request deadline cannot exceed the
operation deadline. Both are comparison parameters recorded in each report;
changing either requires a separately labeled run and never replaces previous
failures. Reports include start/end timestamps and observed cgroup v2 CPU quota
and memory limit, or an explicit unavailable/unsupported status. CPU quota is
reported as the raw `cpu.max` quota/period pair; memory is the raw `memory.max`.

Resource frames include live/retained/pending counts, oldest pending age, registry
counts, RSS, cumulative process CPU seconds, and FDs. Linux `/proc` observations
are marked fresh or unavailable; other platforms explicitly report unsupported.
Removal frames prove the registry loses the retired path; on Linux a bounded check
also requires zero open descriptors referencing its source or sidecars. Process stop is bounded,
waited, and reaped. Before/after final cleanup frames distinguish a running process
from an exited process. CPU counters reset at each process restart. These are
observations, not a claimed universal leak threshold: compare runs at the same
tier and binary, and inspect FD/RSS deltas. Run artifacts intentionally remain for
review and are not part of process cleanup.

Bounds: 2–1000 tenants, at most two generations per tenant, one sequential writer
and restore, 512-byte payloads, ten hot/one cold rows per skew step, a 64 MiB full
log budget, a two-hour run limit, and a configurable per-operation timeout up to
ten minutes. Log overflow and cleanup timeout fail the run. Successful temporary
restores are removed; failed restore artifacts remain. The logical oracle's
existing row/byte/object/value limits also apply. Fleet activation, S3 behavior,
long-duration leak trends, and same-prefix reuse require separate evidence.

Run the real two-tenant smoke explicitly:

```sh
SOAK_TENANT_LITESTREAM_BINARY=/absolute/path/to/reference-litestream \
  GOTOOLCHAIN=go1.25.13 go test ./internal/worker \
  -run TestTenantLifecyclePinnedRestore -count=1 -v
```

Without that variable the integration test reports a skip; it is not a restore
pass. Unit tests still cover fair pending work, generation isolation, bounds,
failed-batch retention, and durable early-error detection.
