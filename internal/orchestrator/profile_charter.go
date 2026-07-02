package orchestrator

import "strings"

// ProfileCharter explains why a workload profile exists in the soak fleet:
// what scenario it runs, what it stresses, the regression it guards against,
// and why catching that regression matters. The workload params in fleet.go
// say what a worker does; the charter says why it is worth doing.
type ProfileCharter struct {
	Synopsis      string
	Stresses      string
	GuardsAgainst string
	WhyItMatters  string
	Known         bool
}

var profileCharters = map[string]ProfileCharter{
	"low-volume": {
		Synopsis:      "The calm baseline: a single writer at 10 writes/s of 1KB rows in the primary region.",
		Stresses:      "Steady-state replication correctness with almost no load on the system.",
		GuardsAgainst: "Fundamental correctness regressions that have nothing to do with load or scale.",
		WhyItMatters:  "It is the control lane. A failure here is unambiguous and isolates Litestream's core logic from any throughput or contention effects.",
	},
	"high-volume": {
		Synopsis:      "Sustained heavy load: 8 writers pushing 500 writes/s of 4KB rows in a wave pattern.",
		Stresses:      "Replication throughput and S3 multipart upload keeping up with high write volume.",
		GuardsAgainst: "Litestream falling behind, dropping, or corrupting LTX when writes outpace replication.",
		WhyItMatters:  "Silent data loss under load is the worst-case failure for a backup tool. This is the primary throughput canary.",
	},
	"low-vol-syd": {
		Synopsis:      "The low-volume baseline, run from Sydney instead of the primary US region.",
		Stresses:      "Replication correctness over a long, high-latency round trip to S3.",
		GuardsAgainst: "Latency-sensitive bugs in timeouts, ordering, or retries that only surface far from the bucket.",
		WhyItMatters:  "Real deployments are not next to their object store. This proves Litestream works across the planet, not just in-datacenter.",
	},
	"high-vol-ams": {
		Synopsis:      "The high-volume throughput lane, run from Amsterdam to add cross-region distance.",
		Stresses:      "Heavy write throughput combined with the higher S3 latency of a remote region.",
		GuardsAgainst: "Throughput regressions that only appear when every upload pays a latency tax.",
		WhyItMatters:  "Confirms high-volume replication holds up outside the low-latency happy path next to the bucket.",
	},
	"burst-volume": {
		Synopsis:      "Spiky load: bursts up to 1000 writes/s of 2KB rows, then quiet, repeating.",
		Stresses:      "How replication absorbs sudden write spikes and drains the backlog afterward.",
		GuardsAgainst: "Backlog and buffer-growth bugs that a constant load never triggers: lag spikes, unbounded buffers, OOM at the peak.",
		WhyItMatters:  "Production traffic is bursty, not smooth. This catches failures that only happen during the spike.",
	},
	"read-heavy": {
		Synopsis:      "Read-dominated load: 95% reads running against an actively replicating database.",
		Stresses:      "Replication and WAL checkpointing running concurrently with heavy read traffic.",
		GuardsAgainst: "Reads being starved, blocked, or served stale while Litestream checkpoints and replicates.",
		WhyItMatters:  "Most applications read far more than they write. This proves replication does not degrade the read path.",
	},
	"gharchive-replay": {
		Synopsis:      "Replays real GitHub Archive event data (large nested JSON) at 300x speed, looping.",
		Stresses:      "Replication of a real-world, irregular insert pattern with large JSON payloads.",
		GuardsAgainst: "Bugs that only appear with production-shaped data, not tidy uniform synthetic rows.",
		WhyItMatters:  "Synthetic load is too regular. Real data has skew and bursts that expose edge cases synthetic load never reaches.",
	},
	"gharchive-mixed": {
		Synopsis:      "Runs synthetic writes and GitHub Archive replay against the same database at once.",
		Stresses:      "Replication driven by two concurrent, differently-shaped write sources.",
		GuardsAgainst: "Interleaving bugs where mixed workloads corrupt each other's pages or confuse the WAL.",
		WhyItMatters:  "Real databases serve many workloads at the same time. This catches interactions a single source cannot.",
	},
	"taxi-mixed": {
		Synopsis:      "Synthetic writes mixed with NYC taxi CSV replay against one database.",
		Stresses:      "Concurrent mixed load using wide, numeric-heavy rows from a real dataset.",
		GuardsAgainst: "Mixed-workload interleaving bugs against a different real-data shape than gharchive.",
		WhyItMatters:  "Different real datasets exercise different code paths: taxi rows are wide and numeric where gharchive is nested JSON.",
	},
	"taxi-replay": {
		Synopsis:      "Replays NYC taxi trip data (wide CSV rows) at 90x speed, looping.",
		Stresses:      "Replication of wide, numeric-heavy rows from a real dataset.",
		GuardsAgainst: "Data-shape-specific bugs tied to row width and column types.",
		WhyItMatters:  "Proves replication stays correct across very different real-world schemas, not just one.",
	},
	"orders-replay": {
		Synopsis:      "Replays a realistic e-commerce orders stream (JSONL) at 45x speed, looping.",
		Stresses:      "Replication of a steady, transactional, order-shaped insert pattern.",
		GuardsAgainst: "Regressions in handling ordinary transactional workloads with relational structure.",
		WhyItMatters:  "Mirrors the most common real Litestream use case: a transactional application database.",
	},
	"many-dbs-100-list": {
		Synopsis:      "Replicates 100 separate SQLite databases, each declared explicitly in the config (list mode).",
		Stresses:      "Managing many simultaneous replication streams enumerated one by one.",
		GuardsAgainst: "Per-database leaks, file-descriptor exhaustion, and scheduling bugs that only appear at fan-out.",
		WhyItMatters:  "Many apps run a database-per-tenant. This proves Litestream scales past a single DB without resource blowup.",
	},
	"many-dbs-100-dir": {
		Synopsis:      "Replicates 100 databases discovered automatically from a directory (dir mode), not listed individually.",
		Stresses:      "Directory-based auto-discovery and replication of many databases.",
		GuardsAgainst: "Discovery bugs (databases missed, double-watched, or mis-globbed) on top of the fan-out resource risks.",
		WhyItMatters:  "Dir mode is how you run many DBs without hand-maintaining config. It must find and replicate every one.",
	},
	"many-dbs-500-list": {
		Synopsis:      "Replicates 500 databases, each declared explicitly in the config (list mode).",
		Stresses:      "Explicitly enumerated replication fan-out at five times the 100-database tier.",
		GuardsAgainst: "Per-database resource costs (memory, file descriptors, S3 request volume) that grow faster than linearly between 100 and 1000 databases.",
		WhyItMatters:  "Fills the gap between the 100 and 1000 lanes, showing where scaling costs bend before they cliff.",
	},
	"many-dbs-500-dir": {
		Synopsis:      "Replicates 500 databases auto-discovered from a directory (dir mode) at default compaction cadence.",
		Stresses:      "Directory discovery plus compaction and retention overhead across 500 replication streams.",
		GuardsAgainst: "Discovery and scheduling regressions at mid-tier fan-out, and background-maintenance cost growth between the 100 and 1000 lanes.",
		WhyItMatters:  "The default-cadence half of the 500-tier pair: its overhead baseline is what the lowfreq control is measured against.",
	},
	"many-dbs-500-dir-lowfreq": {
		Synopsis:      "The reduced-frequency control pair for many-dbs-500-dir: the same 500 databases in dir mode, but with hourly snapshots and relaxed compaction and L0 retention intervals.",
		Stresses:      "500-database replication with deliberately infrequent background maintenance.",
		GuardsAgainst: "Misattributing many-database overhead: comparing against many-dbs-500-dir isolates retention/compaction frequency as the only variable.",
		WhyItMatters:  "Quantifies how much of the S3 LIST, GC, and CPU cost at fan-out comes from the default compaction cadence rather than replication itself.",
	},
	"many-dbs-1000-dir": {
		Synopsis:      "Scale test: 1000 databases auto-discovered from a directory.",
		Stresses:      "Replication fan-out an order of magnitude beyond the 100-database lanes.",
		GuardsAgainst: "Resource and scheduling cliffs that only appear at very high database counts.",
		WhyItMatters:  "Establishes the upper bound for how many databases a single Litestream instance can hold.",
	},
}

// profileCharter returns the charter for a profile name. The lookup tolerates
// the worker-name suffixes that some sources add (for example a trailing region
// tag) by falling back to a longest-prefix match. An unknown profile returns a
// zero-value charter with Known false so callers can omit the panel.
func profileCharter(name string) ProfileCharter {
	name = strings.TrimSpace(name)
	if c, ok := profileCharters[name]; ok {
		c.Known = true
		return c
	}
	return ProfileCharter{}
}

// profileSynopsis returns the one-line synopsis for a profile, or an empty
// string when the profile has no charter.
func profileSynopsis(name string) string {
	return profileCharter(name).Synopsis
}
