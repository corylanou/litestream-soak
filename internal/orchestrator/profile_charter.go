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
	"sentinel-single": {
		Synopsis:      "Always-on single-database sentinel: one writer at 10 writes/s of 1KB rows in the primary region on Litestream's default compaction and retention.",
		Stresses:      "Steady-state replication, snapshots, and restore verification with stock Litestream settings.",
		GuardsAgainst: "Drift in Litestream's default behavior and fundamental correctness regressions that have nothing to do with load or scale.",
		WhyItMatters:  "It is the cheap always-on control lane. A failure here is unambiguous, and keeping defaults untouched means it notices when upstream defaults change.",
	},
	"sentinel-manydb": {
		Synopsis:      "Always-on many-database sentinel: 100 databases auto-discovered from a directory (dir mode) with a rotating 2% active set and relaxed L0 retention checks and compaction intervals.",
		Stresses:      "Directory discovery, per-database lifecycle, and replication fan-out at a request rate tuned to keep S3 cost low.",
		GuardsAgainst: "Discovery, scheduling, and per-database resource regressions that only appear with many databases in one Litestream process.",
		WhyItMatters:  "Database-per-tenant is a common deployment shape. This keeps continuous fan-out coverage on main without paying for default-cadence S3 LIST traffic across 100 databases.",
	},
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
	"overload-truncate0": {
		Synopsis:      "Sustained overload with truncate-page-n: 0, which Litestream maps to its default TRUNCATE backstop of 121,359 pages (about 500 MB at 4 KiB/page): 8 writers at 600 writes/s of 2KB rows.",
		Stresses:      "WAL growth bounding and backlog drain when writes outpace replication while the default TRUNCATE backstop remains engaged.",
		GuardsAgainst: "Unbounded WAL and local L0 backlog growth when chunked sync falls behind or the zero sentinel fails to retain Litestream's default TRUNCATE backstop.",
		WhyItMatters:  "Guards the benbjohnson/litestream#1348, litestream#1343, and litestream#1331 fixes: zero uses the default backstop, and limited sync batches must re-loop until drained, or sustained overload fills the disk.",
	},
	"pinned-reader": {
		Synopsis:      "Steady writes at 200 writes/s while a companion reader repeatedly holds a read transaction open for 4 minutes at a time.",
		Stresses:      "Checkpoint and truncate behavior while a long-lived read transaction pins the WAL above the checkpoint watermark.",
		GuardsAgainst: "Checkpoint starvation, WAL runaway, and wasted passive-checkpoint churn while readers pin the WAL for minutes.",
		WhyItMatters:  "Guards the litestream#1339 fix and the SnapshotReader lock-release contract: long-lived readers are common in reporting workloads and must not degrade replication or stall writers.",
	},
	"gate-stress": {
		Synopsis:      "RC gate stress lane: sustained overload with truncate-page-n: 0 from 8 writers at 600 writes/s of 2KB rows on a 2 CPU / 2 GB machine with a 10 GB volume.",
		Stresses:      "WAL growth bounding and backlog drain under overload, run identically against the release candidate and its baseline.",
		GuardsAgainst: "Release candidates that regress WAL bounding or backlog drain relative to the baseline commit.",
		WhyItMatters:  "Gives each release candidate a paired overload signal against a pinned baseline instead of an open-ended full-fleet clone.",
	},
	"gate-manydb-500": {
		Synopsis:      "RC gate fan-out lane: 500 databases in directory mode with 2% of them active, rotating the active set every 30 minutes from a fixed seed.",
		Stresses:      "Directory discovery, per-database replication streams, and active-set rotation at 500-database fan-out.",
		GuardsAgainst: "Release candidates that leak per-database resources or mishandle rotation at fan-out where the baseline does not.",
		WhyItMatters:  "Database-per-tenant deployments are common; the gate proves a candidate holds up at fan-out before release.",
	},
	"gate-large-db": {
		Synopsis:      "RC gate large-database lane: seeds a 6 GB database on a 40 GB volume with light steady writes and a 2-hour verification cadence with per-phase budgets.",
		Stresses:      "Snapshot upload, multi-GB restore, integrity check, and logical compare on a database far larger than the rest of the fleet.",
		GuardsAgainst: "Memory growth that scales with database size and restore regressions that only appear on multi-GB databases.",
		WhyItMatters:  "Large-database regressions are invisible on the small default fleet databases; the gate checks them against the baseline before release.",
	},
	"gate-remote": {
		Synopsis:      "RC gate remote lane: low-volume writes at 10 writes/s from ams on a 1 CPU / 1 GB machine.",
		Stresses:      "Replication and restore across a long round trip to object storage.",
		GuardsAgainst: "Release candidates that regress latency-sensitive replication paths outside the primary region.",
		WhyItMatters:  "Users replicate from many regions; the gate keeps one remote pairing in every release decision.",
	},
	"gate-vfs": {
		Synopsis:      "RC gate VFS reader lane (opt-in): steady writes at 50 writes/s with 30-second L1 compaction and 10-minute snapshots while a hydrated read-only Litestream VFS connection to the replica holds 4-minute read transactions on an 8 MiB probe table with a 1 MiB page cache, and a second VFS connection is compared with the primary at every verification boundary.",
		Stresses:      "Repeatable reads inside an explicit VFS read transaction while hydration applies new LTX files underneath it, and VFS read correctness against the quiesced primary.",
		GuardsAgainst: "Hydrated readers that see replicated updates mid-transaction once the table outgrows SQLite's page cache, and VFS reads that disagree with the primary at a known txid.",
		WhyItMatters:  "Guards the benbjohnson/litestream#1608 fix: a reporting query that spans several statements must see one snapshot, and only the VFS path exercises hydration under a live writer.",
	},
	"gate-upgrade-lane": {
		Synopsis:      "Upgrade gate lane: runs the baseline Litestream build with a 24-hour snapshot interval so the replica holds only legacy L0-L3 files, then switches to the candidate build on the same volume, database, and replica prefix after the warm-up.",
		Stresses:      "The candidate's compactor, restore planner, and follow restore against a replica the baseline build wrote, with every report attributed to its phase and running Litestream SHA.",
		GuardsAgainst: "Candidates that stop compacting legacy levels, fail to restore pre-upgrade txids, or diverge in follow mode after taking over an existing replica.",
		WhyItMatters:  "Every real deployment upgrades in place; a candidate that only works on a replica it created from scratch is not releasable.",
	},
	"gate-upgrade-lane-nol9": {
		Synopsis:      "Upgrade gate no-L9 lane: the standard upgrade lane, except that after the baseline Litestream process has stopped and before the candidate starts, every L9 object under the lane's own replica prefix is deleted, so the candidate takes over a replica holding only L0-L3.",
		Stresses:      "The candidate's compactor, restore planner, and follow restore on a legacy replica with no snapshot level at all, with the first candidate-written L9 recorded when it appears.",
		GuardsAgainst: "Candidates whose compaction, restore, or follow paths assume an L9 snapshot exists, which the standard lane cannot show because the baseline build always writes an initial L9.",
		WhyItMatters:  "Replicas created by older builds or with snapshots disabled have no L9; a candidate that stalls on them never compacts legacy data.",
	},
	"gate-upgrade-follow": {
		Synopsis:      "Upgrade gate follow lane: steady writes with a 15-minute snapshot interval while a long-running follow restore is compared with the source each cycle after the replica crosses a new L9 snapshot.",
		Stresses:      "Follow-mode restore across L9 snapshot boundaries, compaction progress bounds, and the pinned-txid follow variant, run identically on the candidate and the baseline.",
		GuardsAgainst: "Candidates that route snapshots to L9 in a way follow mode cannot continue through, or that let L0/L1 object counts grow without compaction.",
		WhyItMatters:  "Follow mode is the live-replica reading path; a snapshot layout change that breaks it would be invisible to one-shot restore checks.",
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
