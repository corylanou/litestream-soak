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
Run replacement, decreasing counters, and missing churn observations make
coverage unknown; neither a reset nor a new zero counter clears old errors.
Detailed error events and counter observations can describe the same underlying
failure, so incident counts are evidence observations, not deduplicated root
causes. Workload error totals come from cumulative counters independently.

Maintenance credit requires an attributed completion event named
`maintenance_snapshot_completed`, `maintenance_compaction_completed`, or
`maintenance_retention_completed`. Existing fleet reporters do not yet emit
these events. Their maintenance exposure therefore remains unobserved and they
cannot automatically qualify for clean-soak teardown. Configured intervals and
object counts are not treated as proof that maintenance executed. No fleet,
scenario activation, timeout, or deployment setting is changed here.

Comparison findings are fixed, improved, unchanged, new, regressed, or
inconclusive. Credited findings require complete comparable coverage, matching
workload and profile identities, equal completed-check counts, and measured spans
within one minute. Missing profiles, ambiguous duplicate profile/region pairs,
and incomplete coverage are inconclusive. A regression is not canceled by an
improvement in another profile. Already observed worse results remain worse.

The control plane can preserve only reports it receives. It does not implement a
worker-side delivery outbox. Abrupt/unobserved endings remain unknown. Evidence
journals intentionally have no destructive retention policy; disk growth and
long-lived history query costs need operational monitoring. Data deleted before
this migration cannot be reconstructed and is never inferred from worker age.
