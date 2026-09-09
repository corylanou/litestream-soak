# Binary security evidence

The harness owns soakctl, soakworker, flyctl, and the pinned litestream-test
workload generator. Litestream is the candidate under test. Never modify its
module graph or substitute a different SHA while retaining the original label.

CI scans actual Linux image binaries on every PR and weekly using
`govulncheck v1.7.0`. Each scanner invocation has a 300-second limit; image jobs
have a 35-minute limit. The `security-control-<SHA>` and `security-worker-<SHA>`
artifacts retain binary hashes, source SHAs, compiler/build flags and linked
module versions, raw scanner JSON (including scanner version, database timestamp,
advisory records and traces), stderr, and summaries for 30 days. Download and
archive both artifacts with any long-lived comparison result. Re-running against
a newer vulnerability database can change findings; retain the original JSON.

`summary.json` reports `supported`, `unsupported`, or `error`. Supported means the
scanner completed, not that the binary is secure. A supported scan can contain
findings. Empty/malformed output, timeouts and database failures are errors.
Unsupported candidate binary formats are explicit comparison limitations;
operational unsupported scans and all scan errors fail the check. Raw source
traces distinguish source symbol reachability, imported packages and modules;
binary traces distinguish symbol presence, packages and modules. Neither whole
program reachability nor binary symbol presence proves exploitation by an actual
soak operation. Absence of findings does not establish absence of vulnerabilities.

For a selected candidate or baseline, build its unchanged source using the same
compiler and flags as the worker image, then run from the harness checkout:

```bash
GOTOOLCHAIN=go1.25.13 bash scripts/scan-binary.sh candidate \
  <full-source-sha> /absolute/path/to/litestream /absolute/path/to/evidence
```

Keep the evidence beside the comparison result and identify unsupported scans
when interpreting the comparison. An optional fifth argument supplies the exact
source checkout for an additional source scan and module graph capture. This
checks its HEAD against the supplied SHA and retains dependency diffs. The scan
never edits the candidate. No production credentials are needed for these checks.

## Operational flyctl remediation

The common build selects upstream v0.4.101, commit
`203d7369ecb26c9adecadb501cd95682decdb527`, then changes only x/crypto from v0.55.0
to v0.56.0. The result identifies itself as `0.4.101-soak.1` with a patched commit
suffix and dirty VCS build metadata. The control image retains the dependency
patch, effective go.mod/go.sum, and module graph. CI attaches binary scans;
the optional source scan records the additional call traces.
Deployment jobs use the same build script and compiler. Flyctl retains symbols
for binary scan fidelity; stripped Go 1.26 macOS scans were observed to report
additional module-wide symbols absent from the unstripped scan. This patch belongs only
to harness-owned flyctl; it is never applied to Litestream.

The upstream update removes many findings recorded in #233 and PR #232. The
additional crypto patch fixes GO-2026-6354 and GO-2026-6355, SSH channel deadlocks
that may matter to SSH-backed remote builder connections. The selected candidate
may still contain these advisories; candidate findings remain visible.

The control plane invokes `logs -a <app> --json --no-tail`: upstream selects its
HTTP polling path, not the NATS/WireGuard tailing path, and emits JSON log entries
with level, instance, message, region, timestamp and metadata. Deployments use
remote-only builds, build-only/image-label output, and machine-list JSON. The
build checks required command flags; harness tests verify platform JSON decoding,
event classification, image-reference parsing and deployment snapshots. The
coordinator also verified the patched macOS binary against live Fly APIs:
machine-list JSON identified the started control machine, and buffered JSON logs
exited successfully. No credentials were copied. These checks do not perform a
live deployment; the coordinator owns that validation.

## Reviewed residuals

Only these three advisory IDs are permitted for the flyctl binary at the pinned
source SHA, scoped to the exact modules below. They remain in all raw evidence
and summaries. Any other operational symbol finding fails CI. Package/module-only
findings are retained for review rather than equated with executable paths.

| Advisory | Scope and operational assessment |
| --- | --- |
| GO-2026-4883 | Moby daemon plugin privilege validation. The scanner attributes a path through ioutils initialization; the soak process is a client and does not run a Docker daemon or install daemon plugins. |
| GO-2026-4887 | Moby daemon AuthZ plugin bypass with oversized bodies. Client error handling appears in source traces; the harness does not serve the daemon authorization endpoint. |

The binary scanner also attributes GO-2026-4883/4887 to
`github.com/docker/docker-credential-helpers` (`errCredentialsNotFound.NotFound`
and `Shell.Output`). Those are client helper symbols, not a daemon plugin
implementation; the same narrow assessment applies and the attribution is retained.

| Advisory | Scope and operational assessment |
| --- | --- |
| GO-2026-6225 | `github.com/chrismellard/docker-credential-acr-env` can leak Azure tokens to a malicious registry hostname containing `.azurecr.io`. No fixed version is reported. These jobs use `registry.fly.io`, contain no Azure/AAD credentials, and do not configure ACR credential helpers. Do not use this exception for ACR builds or provide Azure credentials to these processes. |

These exceptions do not assess the remote builder daemon: its patching remains
Fly's responsibility. Reassess these IDs when changing build mode, introducing a
local daemon, adding plugin operations, or updating flyctl. The old docker module
has no fixed version reported for these findings. Do not expand the exceptions
to unrelated modules, binaries, or IDs. Review retained package/module findings
on each tool update and weekly scan. The source advisory records in the artifacts
provide the precise affected versions and upstream references.
