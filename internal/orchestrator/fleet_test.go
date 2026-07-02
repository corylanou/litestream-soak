package orchestrator

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestSourcePRNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		want   int
	}{
		{source: "main", want: 0},
		{source: "pr-1221", want: 1221},
		{source: "pr-0", want: 0},
		{source: "pr-nope", want: 0},
	}

	for _, tc := range tests {
		if got := sourcePRNumber(tc.source); got != tc.want {
			t.Fatalf("sourcePRNumber(%q) = %d, want %d", tc.source, got, tc.want)
		}
	}
}

func TestWorkerNameForSource(t *testing.T) {
	t.Parallel()

	if got := workerNameForSource("main", "worker-main-low-vol"); got != "worker-main-low-vol" {
		t.Fatalf("workerNameForSource(main) = %q", got)
	}
	if got := workerNameForSource("pr-1221", "worker-main-low-vol"); got != "worker-pr-1221-low-vol" {
		t.Fatalf("workerNameForSource(pr-1221) = %q, want worker-pr-1221-low-vol", got)
	}
}

func TestDefaultFleetForSource(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	if len(spec.Workers) == 0 {
		t.Fatal("DefaultFleetForSource() returned no workers")
	}

	first := spec.Workers[0]
	if first.Source != "pr-1221" {
		t.Fatalf("first.Source = %q, want pr-1221", first.Source)
	}
	if first.GitSHA != "soak-sha" {
		t.Fatalf("first.GitSHA = %q, want soak-sha", first.GitSHA)
	}
	if first.LitestreamSHA != "litestream-sha" {
		t.Fatalf("first.LitestreamSHA = %q, want litestream-sha", first.LitestreamSHA)
	}
	if first.PRNumber != 1221 {
		t.Fatalf("first.PRNumber = %d, want 1221", first.PRNumber)
	}
	if first.Name != "worker-pr-1221-low-vol" {
		t.Fatalf("first.Name = %q, want worker-pr-1221-low-vol", first.Name)
	}

	volumeSizes := map[string]int{}
	for _, worker := range spec.Workers {
		if worker.VolumeSizeGB != 0 {
			volumeSizes[worker.ProfileName] = worker.VolumeSizeGB
		}
		if worker.VolumeSizeGB != 0 && worker.Workload.VolumeSizeGB != worker.VolumeSizeGB {
			t.Fatalf("%s Workload.VolumeSizeGB = %d, want %d", worker.ProfileName, worker.Workload.VolumeSizeGB, worker.VolumeSizeGB)
		}
	}
	if got := volumeSizes["high-volume"]; got != 100 {
		t.Fatalf("high-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["burst-volume"]; got != 100 {
		t.Fatalf("burst-volume VolumeSizeGB = %d, want 100", got)
	}
	if got := volumeSizes["gharchive-replay"]; got != 50 {
		t.Fatalf("gharchive-replay VolumeSizeGB = %d, want 50", got)
	}
	if got := volumeSizes["gharchive-mixed"]; got != 50 {
		t.Fatalf("gharchive-mixed VolumeSizeGB = %d, want 50", got)
	}
	if desired, ok := defaultFleetDesiredWorker("pr-1221", "worker-pr-1221-high-vol", "worker-pr-1221-high-vol"); !ok {
		t.Fatal("defaultFleetDesiredWorker() missing PR high-volume worker")
	} else if desired.Name != "worker-pr-1221-high-vol" {
		t.Fatalf("desired.Name = %q, want worker-pr-1221-high-vol", desired.Name)
	}
}

func TestDefaultMainFleetExcludesFixtureSensitiveFaultProfiles(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()

	profiles := map[string]bool{}
	for _, worker := range spec.Workers {
		profiles[worker.ProfileName] = true
	}

	for _, profile := range []string{
		"constrained-disk",
		"compaction-source-stream-drop",
		"uploadpart-retry-quota",
		"provider-http-408",
		"provider-request-canceled",
		"s3-flap",
	} {
		if profiles[profile] {
			t.Fatalf("DefaultMainFleet() includes fixture-sensitive profile %q", profile)
		}
	}
}

func TestDefaultMainFleetKeepsAlwaysOnLoadProfiles(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	profiles := map[string]bool{}
	for _, worker := range spec.Workers {
		profiles[worker.ProfileName] = true
	}

	for _, profile := range []string{
		"low-volume",
		"high-volume",
		"burst-volume",
		"read-heavy",
		"gharchive-replay",
		"gharchive-mixed",
		"taxi-replay",
		"taxi-mixed",
		"orders-replay",
		"low-vol-syd",
		"high-vol-ams",
	} {
		if !profiles[profile] {
			t.Fatalf("DefaultMainFleet() missing always-on load profile %q", profile)
		}
	}
}

func TestDefaultMainFleetIncludesRegionalWorkers(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	regional := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if worker.Region == "ord" {
			continue
		}
		regional[worker.ProfileName] = worker
	}

	lowVol, ok := regional["low-vol-syd"]
	if !ok {
		t.Fatal("DefaultMainFleet() missing low-vol-syd regional worker")
	}
	if lowVol.Region != "syd" {
		t.Fatalf("low-vol-syd Region = %q, want syd", lowVol.Region)
	}
	if lowVol.Workload.WriteRate != 10 || lowVol.Workload.Pattern != "constant" {
		t.Fatalf("low-vol-syd workload = %+v, want low-volume shape", lowVol.Workload)
	}

	highVol, ok := regional["high-vol-ams"]
	if !ok {
		t.Fatal("DefaultMainFleet() missing high-vol-ams regional worker")
	}
	if highVol.Region != "ams" {
		t.Fatalf("high-vol-ams Region = %q, want ams", highVol.Region)
	}
	if highVol.VolumeSizeGB != 100 {
		t.Fatalf("high-vol-ams VolumeSizeGB = %d, want 100", highVol.VolumeSizeGB)
	}
	if highVol.Workload.WriteRate != 500 || highVol.Workload.Pattern != "wave" {
		t.Fatalf("high-vol-ams workload = %+v, want high-volume shape", highVol.Workload)
	}
}

func TestDefaultMainFleetTunesHighVolumeS3Uploads(t *testing.T) {
	t.Parallel()

	spec := DefaultMainFleet()
	highVolume := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		switch worker.ProfileName {
		case "high-volume", "high-vol-ams":
			highVolume[worker.ProfileName] = worker
		}
	}

	for _, profile := range []string{"high-volume", "high-vol-ams"} {
		worker, ok := highVolume[profile]
		if !ok {
			t.Fatalf("DefaultMainFleet() missing %s", profile)
		}
		if worker.Workload.S3PartSize != "16MB" {
			t.Fatalf("%s S3PartSize = %q, want 16MB", profile, worker.Workload.S3PartSize)
		}
		if worker.Workload.S3Concurrency != 8 {
			t.Fatalf("%s S3Concurrency = %d, want 8", profile, worker.Workload.S3Concurrency)
		}
	}
}

func manyDBProfiles(spec FleetSpec) map[string]DesiredWorker {
	many := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if strings.HasPrefix(worker.ProfileName, "many-dbs-") {
			many[worker.ProfileName] = worker
		}
	}
	return many
}

func TestManyDBFleetGating(t *testing.T) {
	tests := []struct {
		name     string
		mainFlag string
		k500     string
		k1000    string
		want     []string
	}{
		{name: "all off", mainFlag: "", k500: "", k1000: "", want: []string{}},
		{name: "main only enables 100 tier", mainFlag: "true", k500: "", k1000: "", want: []string{"many-dbs-100-dir", "many-dbs-100-list"}},
		{name: "500 flag without main is inert", mainFlag: "", k500: "true", k1000: "", want: []string{}},
		{name: "1000 flag without main is inert", mainFlag: "", k500: "", k1000: "true", want: []string{}},
		{name: "main plus 500 adds the 500 tier", mainFlag: "true", k500: "true", k1000: "", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-500-dir", "many-dbs-500-dir-lowfreq", "many-dbs-500-list"}},
		{name: "main plus 1000 enables three", mainFlag: "true", k500: "", k1000: "true", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-1000-dir"}},
		{name: "all flags enable all six", mainFlag: "true", k500: "true", k1000: "true", want: []string{"many-dbs-100-dir", "many-dbs-100-list", "many-dbs-1000-dir", "many-dbs-500-dir", "many-dbs-500-dir-lowfreq", "many-dbs-500-list"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", tc.mainFlag)
			t.Setenv("SOAK_ENABLE_MANY_DB_500", tc.k500)
			t.Setenv("SOAK_ENABLE_MANY_DB_1000", tc.k1000)

			many := manyDBProfiles(DefaultMainFleet())
			got := make([]string, 0, len(many))
			for name := range many {
				got = append(got, name)
			}
			sort.Strings(got)
			want := append([]string{}, tc.want...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("many-dbs profiles = %v, want %v", got, want)
			}
		})
	}
}

func TestDefaultMainFleetIncludesManyDBProfilesWhenEnabled(t *testing.T) {
	t.Setenv("SOAK_ENABLE_MANY_DB_FLEET", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_500", "true")
	t.Setenv("SOAK_ENABLE_MANY_DB_1000", "true")

	spec := DefaultMainFleet()
	many := manyDBProfiles(spec)

	tests := []struct {
		profile      string
		numDatabases int
		configMode   string
		volumeGB     int
		memoryMB     int
		cpus         int
		workers      int
	}{
		{profile: "many-dbs-100-list", numDatabases: 100, configMode: "list", volumeGB: 10, memoryMB: 2048, cpus: 1, workers: 2},
		{profile: "many-dbs-100-dir", numDatabases: 100, configMode: "dir", volumeGB: 10, memoryMB: 2048, cpus: 1, workers: 2},
		{profile: "many-dbs-500-list", numDatabases: 500, configMode: "list", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-500-dir", numDatabases: 500, configMode: "dir", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-500-dir-lowfreq", numDatabases: 500, configMode: "dir", volumeGB: 15, memoryMB: 3072, cpus: 2, workers: 3},
		{profile: "many-dbs-1000-dir", numDatabases: 1000, configMode: "dir", volumeGB: 20, memoryMB: 4096, cpus: 2, workers: 4},
	}

	for _, tc := range tests {
		worker, ok := many[tc.profile]
		if !ok {
			t.Fatalf("DefaultMainFleet() missing %s", tc.profile)
		}
		if worker.Workload.Workers != tc.workers {
			t.Fatalf("%s Workers = %d, want %d", tc.profile, worker.Workload.Workers, tc.workers)
		}
		if worker.Workload.NumDatabases != tc.numDatabases {
			t.Fatalf("%s NumDatabases = %d, want %d", tc.profile, worker.Workload.NumDatabases, tc.numDatabases)
		}
		if worker.Workload.ActivePercent != 2 {
			t.Fatalf("%s ActivePercent = %v, want 2", tc.profile, worker.Workload.ActivePercent)
		}
		if worker.Workload.ActiveRotateInterval != "30m" {
			t.Fatalf("%s ActiveRotateInterval = %q, want 30m", tc.profile, worker.Workload.ActiveRotateInterval)
		}
		if worker.Workload.ActiveSetSeed != 1 {
			t.Fatalf("%s ActiveSetSeed = %d, want 1", tc.profile, worker.Workload.ActiveSetSeed)
		}
		if worker.Workload.ConfigMode != tc.configMode {
			t.Fatalf("%s ConfigMode = %q, want %q", tc.profile, worker.Workload.ConfigMode, tc.configMode)
		}
		if worker.Workload.VerifySampleSize != 5 {
			t.Fatalf("%s VerifySampleSize = %d, want 5", tc.profile, worker.Workload.VerifySampleSize)
		}
		if worker.Workload.VerifyChangedLimit != 100 {
			t.Fatalf("%s VerifyChangedLimit = %d, want 100", tc.profile, worker.Workload.VerifyChangedLimit)
		}
		if worker.VolumeSizeGB != tc.volumeGB || worker.Workload.VolumeSizeGB != tc.volumeGB {
			t.Fatalf("%s volume = %d/%d, want %d", tc.profile, worker.VolumeSizeGB, worker.Workload.VolumeSizeGB, tc.volumeGB)
		}
		if worker.Workload.MemoryMB != tc.memoryMB {
			t.Fatalf("%s MemoryMB = %d, want %d", tc.profile, worker.Workload.MemoryMB, tc.memoryMB)
		}
		if worker.Workload.CPUs != tc.cpus {
			t.Fatalf("%s CPUs = %d, want %d", tc.profile, worker.Workload.CPUs, tc.cpus)
		}
	}

	for profile, worker := range many {
		if !worker.Workload.S3FaultProxyEnabled {
			t.Fatalf("%s S3FaultProxyEnabled = false, want true", profile)
		}
		if worker.Workload.S3FaultProxyMode != "observe" {
			t.Fatalf("%s S3FaultProxyMode = %q, want observe", profile, worker.Workload.S3FaultProxyMode)
		}
	}

	lowfreq := many["many-dbs-500-dir-lowfreq"]
	if lowfreq.Workload.SnapshotInterval != "1h" {
		t.Fatalf("lowfreq SnapshotInterval = %q, want 1h", lowfreq.Workload.SnapshotInterval)
	}
	if lowfreq.Workload.L1CompactionInterval != "5m" {
		t.Fatalf("lowfreq L1CompactionInterval = %q, want 5m", lowfreq.Workload.L1CompactionInterval)
	}
	if lowfreq.Workload.L2CompactionInterval != "30m" {
		t.Fatalf("lowfreq L2CompactionInterval = %q, want 30m", lowfreq.Workload.L2CompactionInterval)
	}
	if lowfreq.Workload.L3CompactionInterval != "6h" {
		t.Fatalf("lowfreq L3CompactionInterval = %q, want 6h", lowfreq.Workload.L3CompactionInterval)
	}
	if lowfreq.Workload.L0Retention != "1h" {
		t.Fatalf("lowfreq L0Retention = %q, want 1h", lowfreq.Workload.L0Retention)
	}
	if lowfreq.Workload.L0RetentionCheckInterval != "2m" {
		t.Fatalf("lowfreq L0RetentionCheckInterval = %q, want 2m", lowfreq.Workload.L0RetentionCheckInterval)
	}

	for _, profile := range []string{"many-dbs-500-list", "many-dbs-500-dir"} {
		w := many[profile].Workload
		if w.L1CompactionInterval != "" || w.L2CompactionInterval != "" || w.L3CompactionInterval != "" ||
			w.L0Retention != "" || w.L0RetentionCheckInterval != "" {
			t.Fatalf("%s compaction/retention knobs = %q/%q/%q/%q/%q, want all empty",
				profile, w.L1CompactionInterval, w.L2CompactionInterval, w.L3CompactionInterval,
				w.L0Retention, w.L0RetentionCheckInterval)
		}
	}
}

func TestManyDBProfilesExcludedFromReleaseQuality(t *testing.T) {
	t.Parallel()

	if workerIncludedInReleaseQuality(model.Worker{ProfileName: "many-dbs-100-dir", Region: "ord"}) {
		t.Fatal("many-dbs-100-dir should be excluded from release quality")
	}
	if workerIncludedInReleaseQuality(model.Worker{ProfileName: "many-dbs-500-dir-lowfreq", Region: "ord"}) {
		t.Fatal("many-dbs-500-dir-lowfreq should be excluded from release quality")
	}
	if !workerIncludedInReleaseQuality(model.Worker{ProfileName: "low-volume", Region: "ord"}) {
		t.Fatal("low-volume in ord should be included in release quality")
	}
}

func TestDefaultFleetForSourceRewritesRegionalWorkers(t *testing.T) {
	t.Parallel()

	spec := DefaultFleetForSource("pr-1221", "soak-sha", "litestream-sha")
	regional := map[string]DesiredWorker{}
	for _, worker := range spec.Workers {
		if worker.Region == "ord" {
			continue
		}
		regional[worker.ProfileName] = worker
	}

	lowVol, ok := regional["low-vol-syd"]
	if !ok {
		t.Fatal("DefaultFleetForSource() missing low-vol-syd regional worker")
	}
	if lowVol.WorkerID != "worker-pr-1221-low-vol-syd" {
		t.Fatalf("low-vol-syd WorkerID = %q, want worker-pr-1221-low-vol-syd", lowVol.WorkerID)
	}
	if lowVol.Name != "worker-pr-1221-low-vol-syd" {
		t.Fatalf("low-vol-syd Name = %q, want worker-pr-1221-low-vol-syd", lowVol.Name)
	}
	if lowVol.Region != "syd" {
		t.Fatalf("low-vol-syd Region = %q, want syd", lowVol.Region)
	}
}

func TestResolveWorkerVolumeSizeUsesDefaultFleetForRollouts(t *testing.T) {
	t.Parallel()

	worker := model.Worker{
		ID:            "worker-pr-1228-high-vol",
		Name:          "worker-pr-1228-high-vol",
		Source:        "pr-1228",
		ProfileName:   "high-volume",
		ProfileConfig: workload.Config{LoadMode: "synthetic"}.JSON(),
	}

	parsedCfg, err := workload.ParseConfig(worker.ProfileConfig)
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v, want nil", worker.ProfileConfig, err)
	}
	if got := resolveWorkerVolumeSize(worker, normalizeWorkload(parsedCfg)); got != 100 {
		t.Fatalf("resolveWorkerVolumeSize() = %d, want 100", got)
	}
}
