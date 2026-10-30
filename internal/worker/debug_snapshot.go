package worker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

const (
	debugCommandTimeout     = 5 * time.Second
	debugObjectLevelTimeout = 2 * time.Second
	debugOutputLimit        = 64 * 1024
	debugLineLimit          = 80
	debugObjectCountCap     = 5000
)

func (r *Runner) captureFailureDebugSnapshot(reason string, steps []reporting.VerificationStep, classification *reporting.FailureClassification, restoreTXID uint64) *reporting.FailureDebugSnapshot {
	snapshot := &reporting.FailureDebugSnapshot{
		CapturedAt:            time.Now().UTC(),
		Reason:                reason,
		Run:                   r.identity,
		FailureClassification: classification,
		ProcessTable:          collectProcessTable(),
		SocketSummary:         collectSocketSummary(r.cfg.SocketPath),
		Disk:                  collectDiskSnapshot(r.cfg),
		Cgroup:                collectCgroupSnapshot(),
		LitestreamExit:        r.litestreamExitSnapshot(),
		VerificationSteps:     append([]reporting.VerificationStep(nil), steps...),
		LitestreamLogTail:     r.litestreamLog.Lines(),
		LoadLogTail:           r.loadLog.Lines(),
	}
	snapshot.FDCounts = collectFDCounts(snapshot.ProcessTable, r.cfg)
	snapshot.CommandOutputs = []reporting.CommandOutput{
		runDebugCommand("verification_log_tail", "tail", "-n", strconv.Itoa(debugLineLimit), filepath.Join(r.cfg.DataDir, "verification.log")),
		runDebugCommand("litestream_config", "sh", "-c", "sed -n '1,220p' "+shellQuote(r.cfg.ConfigPath)),
	}
	if restoreTXID > 0 {
		snapshot.RestorePlan = collectRestorePlanSnapshot(r.cfg, restoreTXID)
	}
	if r.cfg.ReplicaType == "s3" {
		snapshot.ObjectStoragePrefix = collectObjectStoragePrefix(r.cfg)
	}
	return snapshot
}

func collectProcessTable() []reporting.ProcessSnapshot {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}

	processes := make([]reporting.ProcessSnapshot, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		status := readProcStatus(pid)
		command := strings.TrimSpace(readFileString(filepath.Join("/proc", entry.Name(), "comm")))
		args := strings.Join(splitCmdline(readFileString(filepath.Join("/proc", entry.Name(), "cmdline"))), " ")
		if !interestingProcess(command, args) {
			continue
		}
		processes = append(processes, reporting.ProcessSnapshot{
			PID:     pid,
			PPID:    parseStatusInt(status, "PPid"),
			State:   parseStatusString(status, "State"),
			Command: command,
			Args:    args,
		})
	}
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].PID < processes[j].PID
	})
	return processes
}

func interestingProcess(command, args string) bool {
	text := strings.ToLower(command + " " + args)
	return strings.Contains(text, "soakworker") ||
		strings.Contains(text, "litestream") ||
		strings.Contains(text, "litestream-test")
}

func collectFDCounts(processes []reporting.ProcessSnapshot, cfg Config) []reporting.ProcessFDCounts {
	counts := make([]reporting.ProcessFDCounts, 0, len(processes))
	for _, process := range processes {
		count := reporting.ProcessFDCounts{
			PID:     process.PID,
			Command: process.Command,
			ByType:  make(map[string]int),
		}
		entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(process.PID), "fd"))
		if err != nil {
			count.Error = err.Error()
			counts = append(counts, count)
			continue
		}
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(process.PID), "fd", entry.Name()))
			if err != nil {
				continue
			}
			fdType := classifyFDTarget(target, cfg)
			count.ByType[fdType]++
			count.Total++
			if len(count.Samples) < 20 {
				count.Samples = append(count.Samples, entry.Name()+" -> "+target)
			}
		}
		counts = append(counts, count)
	}
	return counts
}

func classifyFDTarget(target string, cfg Config) string {
	switch {
	case strings.HasPrefix(target, "socket:"):
		return "socket"
	case target == cfg.DBPath:
		return "db"
	case target == cfg.DBPath+"-wal":
		return "wal"
	case strings.Contains(target, ".ltx"):
		return "ltx"
	case strings.Contains(target, "litestream.sock"):
		return "litestream_socket"
	case strings.Contains(target, "test.db-litestream") || strings.Contains(target, ".test.db-litestream"):
		return "litestream_local_state"
	case strings.HasPrefix(target, "/data"):
		return "data_file"
	default:
		return "other"
	}
}

func collectSocketSummary(socketPath string) reporting.SocketSummary {
	output := runDebugCommand("socket_summary", "ss", "-xap")
	summary := reporting.SocketSummary{Path: socketPath}
	if output.Error != "" {
		summary.Error = output.Error
		return summary
	}
	for _, line := range strings.Split(output.Output, "\n") {
		if strings.Contains(line, socketPath) {
			summary.LineCount++
			if len(summary.SampleLines) < 40 {
				summary.SampleLines = append(summary.SampleLines, line)
			}
		}
	}
	return summary
}

func collectDiskSnapshot(cfg Config) reporting.DiskSnapshot {
	snapshot := reporting.DiskSnapshot{
		DataDir:   cfg.DataDir,
		DataFiles: make(map[string]string),
	}
	for _, path := range []string{cfg.DBPath, cfg.DBPath + "-wal", litestreamStateDir(cfg.DBPath), cfg.SocketPath} {
		snapshot.DataFiles[path] = pathSummary(path)
	}
	if output := runDebugCommand("df_data", "df", "-h", cfg.DataDir); output.Error == "" {
		snapshot.DataUsage = output.Output
	} else {
		snapshot.Error = output.Error
	}
	if output := runDebugCommand("largest_data_paths", "sh", "-c", "du -sh "+shellQuote(cfg.DataDir)+"/* "+shellQuote(litestreamStateDir(cfg.DBPath))+" 2>/dev/null | sort -h | tail -n 40"); output.Error == "" {
		snapshot.LargestPaths = nonEmptyLines(output.Output)
	}
	return snapshot
}

func collectCgroupSnapshot() reporting.CgroupSnapshot {
	snapshot := reporting.CgroupSnapshot{
		MemoryCurrent: readFileString("/sys/fs/cgroup/memory.current"),
		MemoryMax:     readFileString("/sys/fs/cgroup/memory.max"),
		PidsCurrent:   readFileString("/sys/fs/cgroup/pids.current"),
		PidsMax:       readFileString("/sys/fs/cgroup/pids.max"),
		CPUStat:       parseKeyValueFile("/sys/fs/cgroup/cpu.stat"),
	}
	return snapshot
}

func collectObjectStoragePrefix(cfg Config) *reporting.ObjectStoragePrefixSnapshot {
	prefixURL := fmt.Sprintf("s3://%s/%s/", cfg.S3Bucket, strings.Trim(strings.TrimPrefix(cfg.S3Path, "/"), "/"))
	snapshot := &reporting.ObjectStoragePrefixSnapshot{
		URL:         prefixURL,
		LevelCounts: make(map[string]int),
	}
	host := endpointHost(cfg.directS3Endpoint())
	for level := 9; level >= 0; level-- {
		levelName := fmt.Sprintf("%04d", level)
		levelURL := prefixURL + levelName + "/"
		listing := collectObjectStorageLevel(host, levelName, levelURL)
		snapshot.LevelListings = append(snapshot.LevelListings, listing)
		if listing.Truncated {
			snapshot.CommandTruncated = true
		}
		if listing.Error != "" && snapshot.Error == "" {
			snapshot.Error = fmt.Sprintf("%s: %s", listing.Level, listing.Error)
		}
		snapshot.ObjectCount += listing.ObjectCount
		snapshot.TotalBytes += listing.TotalBytes
		snapshot.LevelCounts[listing.Level] = listing.ObjectCount
		snapshot.LatestObjects = append(snapshot.LatestObjects, listing.LatestObjects...)
		snapshot.LatestObjects = tailLines(snapshot.LatestObjects, 20)
	}
	return snapshot
}

func collectObjectStorageLevel(host, level, levelURL string) reporting.ObjectStorageLevelSnapshot {
	command := fmt.Sprintf(
		`s3cmd --access_key="$AWS_ACCESS_KEY_ID" --secret_key="$AWS_SECRET_ACCESS_KEY" --host=%s --host-bucket='%%(bucket)s.%s' --region="$AWS_REGION" ls --recursive %s`,
		shellQuote(host),
		host,
		shellQuote(levelURL),
	)
	start := time.Now()
	output := runDebugCommandWithTimeout("object_storage_level_"+level, debugObjectLevelTimeout, "sh", "-c", command)
	listing := reporting.ObjectStorageLevelSnapshot{
		Level:      level,
		URL:        levelURL,
		DurationMS: int(time.Since(start).Milliseconds()),
		TimedOut:   output.Error == "command timed out",
		Truncated:  output.Truncated,
		Error:      output.Error,
	}
	for _, line := range nonEmptyLines(output.Output) {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		if listing.ObjectCount >= debugObjectCountCap {
			listing.ObjectCountCapped = true
			continue
		}
		size, _ := strconv.ParseInt(fields[2], 10, 64)
		listing.ObjectCount++
		listing.TotalBytes += size
		listing.LatestObjects = append(listing.LatestObjects, line)
		listing.LatestObjects = tailLines(listing.LatestObjects, 10)
	}
	if listing.ObjectCount > 0 {
		listing.PageCount = (listing.ObjectCount + 999) / 1000
	}
	if output.Truncated {
		listing.ObjectCountCapped = true
	}
	return listing
}

type restorePlanCandidate struct {
	level     int
	minTXID   uint64
	maxTXID   uint64
	sizeBytes int64
	createdAt string
}

func collectRestorePlanSnapshot(cfg Config, targetTXID uint64) *reporting.RestorePlanSnapshot {
	command := []string{cfg.litestreamCommand(), "ltx", "-config", cfg.ConfigPath, "-level", "all", cfg.DBPath}
	listing := listReplicaLTX(command)
	snapshot := &reporting.RestorePlanSnapshot{
		CapturedAt:        time.Now().UTC(),
		Command:           command,
		TargetTXID:        formatTXID(targetTXID),
		TargetTXIDDecimal: targetTXID,
	}
	candidates := listing.candidates
	snapshot.CandidateCount = len(candidates)
	if listing.err != "" || !listing.complete {
		// A listing that failed part-way may still have emitted entries that
		// cover the target; a plan picked from them would be a guess, so the
		// snapshot stays incomplete with the partial input flagged.
		snapshot.Error = firstNonEmpty(listing.err, "listing incomplete")
		snapshot.OutputTail = listing.tail
		snapshot.Truncated = len(candidates) > 0
		return snapshot
	}
	if len(candidates) == 0 {
		snapshot.Error = "no ltx entries parsed"
		snapshot.OutputTail = listing.tail
		return snapshot
	}

	selected := selectRestorePlanCandidates(candidates, targetTXID)
	snapshot.Entries = restorePlanEntries(selected)
	snapshot.Complete = restorePlanComplete(selected, targetTXID)
	if !snapshot.Complete && snapshot.Error == "" {
		snapshot.Error = "restore plan did not cover target txid"
	}
	return snapshot
}

// collectReplicaLevelReport summarizes every LTX object on the replica by
// level. The listing is parsed as a stream with no size cap, so the summary is
// built from the whole listing however long it is; only the raw evidence kept
// in OutputTail is bounded. Complete is set only when the whole listing was
// read and the command exited cleanly, and consumers must not judge compaction
// or L9 progress from a report that is not Complete.
func collectReplicaLevelReport(cfg Config, targetTXID uint64) *reporting.ReplicaLevelReport {
	command := []string{cfg.litestreamCommand(), "ltx", "-config", cfg.ConfigPath, "-level", "all", cfg.DBPath}
	listing := listReplicaLTX(command)
	report := &reporting.ReplicaLevelReport{
		CapturedAt:        time.Now().UTC(),
		Command:           command,
		TargetTXID:        formatTXID(targetTXID),
		TargetTXIDDecimal: targetTXID,
		Levels:            replicaLevelSummaries(listing.candidates),
		Complete:          listing.complete,
		Error:             listing.err,
	}
	if report.Error == "" && len(listing.candidates) == 0 {
		report.Error = "no ltx entries parsed"
	}
	if report.Error != "" {
		report.OutputTail = listing.tail
		report.Complete = false
	}
	return report
}

// replicaLTXListing is the streamed result of one `litestream ltx -level all`
// run: every parsed entry, a bounded tail of the raw output for evidence, and
// whether the whole listing was read.
type replicaLTXListing struct {
	candidates []restorePlanCandidate
	tail       string
	complete   bool
	err        string
}

const (
	replicaLTXListingTimeout = 2 * time.Minute
	replicaLTXListingTail    = 8192
	replicaLTXMaxLineBytes   = 1 << 20
)

func listReplicaLTX(command []string) replicaLTXListing {
	ctx, cancel := context.WithTimeout(context.Background(), replicaLTXListingTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	var stderr bytes.Buffer
	cmd.Stderr = &limitedBuffer{buf: &stderr, limit: debugOutputLimit / 4}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return replicaLTXListing{err: err.Error()}
	}
	if err := cmd.Start(); err != nil {
		return replicaLTXListing{err: err.Error()}
	}
	listing := replicaLTXListing{candidates: make([]restorePlanCandidate, 0)}
	tail := &tailBuffer{limit: replicaLTXListingTail}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), replicaLTXMaxLineBytes)
	for scanner.Scan() {
		line := scanner.Text()
		tail.add(line)
		if candidate, ok := parseRestorePlanLine(line); ok {
			listing.candidates = append(listing.candidates, candidate)
		}
	}
	scanErr := scanner.Err()
	waitErr := cmd.Wait()
	listing.tail = tail.String()
	if stderr.Len() > 0 {
		listing.tail = strings.TrimRight(listing.tail, "\n") + "\nstderr:\n" + stderr.String()
	}
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		listing.err = "command timed out"
	case scanErr != nil:
		listing.err = "read listing: " + scanErr.Error()
	case waitErr != nil:
		listing.err = waitErr.Error()
	default:
		listing.complete = true
	}
	return listing
}

// tailBuffer keeps the last limit bytes of appended lines.
type tailBuffer struct {
	limit int
	lines []string
	size  int
}

func (b *tailBuffer) add(line string) {
	b.lines = append(b.lines, line)
	b.size += len(line) + 1
	for b.size > b.limit && len(b.lines) > 1 {
		b.size -= len(b.lines[0]) + 1
		b.lines = b.lines[1:]
	}
}

func (b *tailBuffer) String() string {
	return strings.Join(b.lines, "\n")
}

func parseRestorePlanCandidates(output string) []restorePlanCandidate {
	candidates := make([]restorePlanCandidate, 0)
	for _, line := range nonEmptyLines(output) {
		if candidate, ok := parseRestorePlanLine(line); ok {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

func parseRestorePlanLine(line string) (restorePlanCandidate, bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return restorePlanCandidate{}, false
	}
	level, err := strconv.Atoi(fields[0])
	if err != nil {
		return restorePlanCandidate{}, false
	}
	minTXID, err := strconv.ParseUint(fields[1], 16, 64)
	if err != nil {
		return restorePlanCandidate{}, false
	}
	maxTXID, err := strconv.ParseUint(fields[2], 16, 64)
	if err != nil {
		return restorePlanCandidate{}, false
	}
	sizeBytes, _ := strconv.ParseInt(fields[3], 10, 64)
	return restorePlanCandidate{
		level:     level,
		minTXID:   minTXID,
		maxTXID:   maxTXID,
		sizeBytes: sizeBytes,
		createdAt: fields[4],
	}, true
}

func replicaLevelSummaries(candidates []restorePlanCandidate) []reporting.ReplicaLevelSummary {
	summaries := make([]reporting.ReplicaLevelSummary, 10)
	for level := 0; level <= 9; level++ {
		summaries[level] = reporting.ReplicaLevelSummary{
			Level:     level,
			LevelName: fmt.Sprintf("%04d", level),
		}
	}
	for _, candidate := range candidates {
		if candidate.level < 0 || candidate.level >= len(summaries) {
			continue
		}
		summary := &summaries[candidate.level]
		if summary.ObjectCount == 0 || candidate.minTXID < summary.MinTXIDDecimal {
			summary.MinTXIDDecimal = candidate.minTXID
			summary.MinTXID = formatTXID(candidate.minTXID)
		}
		if candidate.maxTXID > summary.MaxTXIDDecimal {
			summary.MaxTXIDDecimal = candidate.maxTXID
			summary.MaxTXID = formatTXID(candidate.maxTXID)
		}
		summary.ObjectCount++
		summary.TotalBytes += candidate.sizeBytes
	}
	return summaries
}

func selectRestorePlanCandidates(candidates []restorePlanCandidate, targetTXID uint64) []restorePlanCandidate {
	if targetTXID == 0 {
		return nil
	}

	selected := make([]restorePlanCandidate, 0)
	currentTXID := uint64(0)
	if snapshot, ok := latestBaseSnapshot(candidates, targetTXID); ok {
		selected = append(selected, snapshot)
		currentTXID = snapshot.maxTXID
	}

	for currentTXID < targetTXID {
		next, ok := bestRestorePlanNextCandidate(candidates, currentTXID, targetTXID)
		if !ok {
			break
		}
		selected = append(selected, next)
		currentTXID = next.maxTXID
	}
	return selected
}

func latestBaseSnapshot(candidates []restorePlanCandidate, targetTXID uint64) (restorePlanCandidate, bool) {
	var best restorePlanCandidate
	ok := false
	for _, candidate := range candidates {
		if candidate.level != 9 || candidate.minTXID > 1 || candidate.maxTXID > targetTXID {
			continue
		}
		if !ok || candidate.maxTXID > best.maxTXID {
			best = candidate
			ok = true
		}
	}
	return best, ok
}

func bestRestorePlanNextCandidate(candidates []restorePlanCandidate, currentTXID, targetTXID uint64) (restorePlanCandidate, bool) {
	nextTXID := currentTXID + 1
	var best restorePlanCandidate
	ok := false
	for _, candidate := range candidates {
		if candidate.minTXID > nextTXID || candidate.maxTXID <= currentTXID || candidate.maxTXID > targetTXID {
			continue
		}
		if !ok ||
			candidate.maxTXID > best.maxTXID ||
			(candidate.maxTXID == best.maxTXID && candidate.level > best.level) ||
			(candidate.maxTXID == best.maxTXID && candidate.level == best.level && candidate.minTXID < best.minTXID) {
			best = candidate
			ok = true
		}
	}
	return best, ok
}

func restorePlanComplete(selected []restorePlanCandidate, targetTXID uint64) bool {
	if len(selected) == 0 || targetTXID == 0 {
		return false
	}
	return selected[len(selected)-1].maxTXID >= targetTXID
}

func restorePlanEntries(candidates []restorePlanCandidate) []reporting.RestorePlanEntry {
	entries := make([]reporting.RestorePlanEntry, 0, len(candidates))
	for _, candidate := range candidates {
		minTXID := formatTXID(candidate.minTXID)
		maxTXID := formatTXID(candidate.maxTXID)
		levelName := fmt.Sprintf("%04d", candidate.level)
		entries = append(entries, reporting.RestorePlanEntry{
			Level:      candidate.level,
			LevelName:  levelName,
			MinTXID:    minTXID,
			MaxTXID:    maxTXID,
			SizeBytes:  candidate.sizeBytes,
			CreatedAt:  candidate.createdAt,
			ObjectPath: fmt.Sprintf("%s/%s-%s.ltx", levelName, minTXID, maxTXID),
		})
	}
	return entries
}

func runDebugCommand(name string, command string, args ...string) reporting.CommandOutput {
	return runDebugCommandWithTimeout(name, debugCommandTimeout, command, args...)
}

func runDebugCommandWithTimeout(name string, timeout time.Duration, command string, args ...string) reporting.CommandOutput {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, command, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &limitedBuffer{buf: &stdout, limit: debugOutputLimit}
	cmd.Stderr = &limitedBuffer{buf: &stderr, limit: debugOutputLimit / 4}
	err := cmd.Run()

	output := stdout.String()
	if stderr.Len() > 0 {
		output = strings.TrimRight(output, "\n") + "\nstderr:\n" + stderr.String()
	}
	result := reporting.CommandOutput{
		Name:      name,
		Output:    output,
		Truncated: stdout.Len() >= debugOutputLimit || stderr.Len() >= debugOutputLimit/4,
	}
	if ctx.Err() == context.DeadlineExceeded {
		result.Error = "command timed out"
		return result
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

type limitedBuffer struct {
	buf   *bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		return len(p), nil
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func readProcStatus(pid int) map[string]string {
	return parseKeyValueFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
}

func parseKeyValueFile(path string) map[string]string {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = file.Close() }()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				values[fields[0]] = strings.Join(fields[1:], " ")
			}
			continue
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return values
}

func parseStatusInt(status map[string]string, key string) int {
	value, _ := strconv.Atoi(firstField(status[key]))
	return value
}

func parseStatusString(status map[string]string, key string) string {
	return status[key]
}

func firstField(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func splitCmdline(raw string) []string {
	parts := strings.Split(raw, "\x00")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			out = append(out, part)
		}
	}
	return out
}

func readFileString(path string) string {
	body, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

func pathSummary(path string) string {
	info, err := os.Lstat(path)
	if err != nil {
		return err.Error()
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, _ := os.Readlink(path)
		return fmt.Sprintf("symlink -> %s", target)
	}
	if info.IsDir() {
		return fmt.Sprintf("dir mode=%s", info.Mode())
	}
	return fmt.Sprintf("file size=%d mode=%s", info.Size(), info.Mode())
}

func nonEmptyLines(output string) []string {
	lines := strings.Split(output, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func tailLines(lines []string, limit int) []string {
	if len(lines) <= limit {
		return lines
	}
	return lines[len(lines)-limit:]
}

func endpointHost(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err == nil && parsed.Host != "" {
		return parsed.Host
	}
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
