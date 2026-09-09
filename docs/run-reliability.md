# Complete-run reliability

Deployment comparisons expose `reliability` alongside the existing latest-result
scorecard, and `reliability_findings` alongside its deltas. The dashboard displays
current verification health separately from retained complete-run evidence.
Regional and many-database profiles participate in this evidence even when the
legacy release score excludes them. Issues #187 and #192 remain open ownership
references, not exemptions or expected-failure classifications.

Verification and event inserts are journaled atomically. Updating a coalesced
event retains its earlier observations. Accepted runtime reports are journaled
with immutable reporter identity and server-computed attribution. Journals are
independent of worker rows, raw-history retention, dormancy, and run archives.
Deleting or recreating a worker does not delete these journals. Migrated retained
records remain readable but cannot prove that earlier history was complete.

A failed verification, reported retry, recovered platform incident, or workload
error remains visible after recovery. An explicit `fault_injection_engaged`
event is classified separately; it never excuses a verification failure or makes
a run eligible for a clean soak. Pending verification is neutral: it adds no
failure or completed verification, and latest pending status blocks current
coverage eligibility. Unattributed and unavailable observations remain visible
without comparison credit.

Clean-soak eligibility requires at least two attributed completed verifications,
a measured span covering the configured success threshold (24 hours in the
comparison report), and no leading, internal, or trailing gap greater than one
hour. Active runs use the current time for their trailing gap. It also requires
observed workload progress, maintenance exposure, complete attribution/history,
a current pass, and no unexpected incidents or incomplete observations. Worker
age, deployment age, a recovered latest result, and an old success archive cannot
supply missing coverage.

Transaction progress is measured between fresh runtime observations in the same
run. Churn reports additionally retain `workload_attempts_total`,
`workload_mutations_total`, `workload_busy_total`, and `workload_errors_total`
when `workload_counters_present` is true. Errors include busy errors; the busy
counter is a subset. Repeated cumulative samples contribute only their increases within each immutable
run identity and `workload_counter_epoch`. Epoch maxima prevent at-least-once
outbox replay from inflating totals. Missing epochs leave coverage unknown.
Forward-time heartbeats detect process replacement and genuine counter resets;
late immutable events contribute epoch maxima without moving current health or
liveness backward. Exact event IDs deduplicate delivery retries. Queue/cache
requirements come from the workload configuration's load mode.

Maintenance exposure comes from observed successful Litestream INFO logs:
positive-size snapshot and compaction completions, and retention completions
with a positive deletion count. All three kinds are required. Configured
intervals, startup messages, attempts, and zero-deletion retention earn no
credit. Missing or incomplete observation leaves eligibility unknown.

Individual WARN/ERROR and retry/failure logs, including INFO reports, are redacted and persisted before notification to the
shared background evidence uploader. Each carries original run identity and a
unique observer-epoch/sequence ID. Unknown log formats invalidate observation
completeness; valid routine DEBUG records are neutral. Process exit flushes any
unterminated final line after the writers finish and before final delivery. The bounded outbox uses durable file and
directory synchronization; failed persistence or exhausted capacity cancels the
workload. Network delivery never runs in the log writer. Restart replays pending
incidents without replacing their identity. Cumulative log counters preserve
failure totals while exact events retain each original diagnostic; delivery
retries and counter/event overlap do not inflate totals.

Comparison findings are fixed, improved, unchanged, new, regressed, or
inconclusive. Credited findings require complete comparable coverage, matching
workload and profile identities, equal completed-check counts, and measured spans
within one minute. Missing profiles, ambiguous duplicate profile/region pairs,
and incomplete coverage are inconclusive. A regression is not canceled by an
improvement in another profile. Already observed worse results remain worse.

The control plane can preserve only reports it receives. The worker outbox
survives process restart on retained storage, but cannot recover a destroyed
volume or an incident that could not be persisted. Abrupt/unobserved endings
remain unknown. Evidence
journals intentionally have no destructive retention policy; disk growth and
long-lived history query costs need operational monitoring. Data deleted before
this migration cannot be reconstructed and is never inferred from worker age.
