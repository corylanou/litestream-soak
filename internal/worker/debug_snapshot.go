package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
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
		Run:                   r.debugIdentity(),
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

func (r *Runner) debugIdentity() reporting.WorkerIdentity {
	profileConfig := r.cfg.WorkloadConfig().JSON()
	return reporting.WorkerIdentity{
		WorkerID:      r.cfg.WorkerID,
		Name:          r.cfg.WorkerName,
		Source:        r.cfg.Source,
		GitSHA:        r.cfg.GitSHA,
		LitestreamSHA: r.cfg.LitestreamSHA,
		RunID:         r.cfg.RunID,
		ImageRef:      r.cfg.ImageRef,
		VolumeID:      r.cfg.VolumeID,
		VolumeSizeGB:  r.cfg.VolumeSizeGB,
		ProfileName:   r.cfg.ProfileName,
		ProfileConfig: profileConfig,
		ProfileHash:   profileHash(profileConfig),
		AppName:       r.cfg.AppName,
		MachineID:     r.cfg.MachineID,
		Region:        r.cfg.Region,
	}
}

func (r *Runner) litestreamExitSnapshot() *reporting.ProcessExitSnapshot {
	r.litestreamMu.Lock()
	defer r.litestreamMu.Unlock()
	if r.litestreamExit == nil {
		return nil
	}
	exit := *r.litestreamExit
	return &exit
}

func processExitSnapshot(process string, exitedAt time.Time, err error) *reporting.ProcessExitSnapshot {
	snapshot := &reporting.ProcessExitSnapshot{
		Process:  process,
		ExitedAt: exitedAt,
	}
	if err == nil {
		code := 0
		snapshot.ExitCode = &code
		return snapshot
	}
	snapshot.Error = err.Error()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		snapshot.ExitCode = &code
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			snapshot.Signal = status.Signal().String()
		}
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
	host := endpointHost(cfg.S3Endpoint)
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
	command := []string{"litestream", "ltx", "-config", cfg.ConfigPath, "-level", "all", cfg.DBPath}
	output := runDebugCommand("restore_plan_ltx_listing", command[0], command[1:]...)
	snapshot := &reporting.RestorePlanSnapshot{
		CapturedAt:        time.Now().UTC(),
		Command:           command,
		TargetTXID:        formatTXID(targetTXID),
		TargetTXIDDecimal: targetTXID,
		Truncated:         output.Truncated,
	}
	candidates := parseRestorePlanCandidates(output.Output)
	snapshot.CandidateCount = len(candidates)
	if output.Error != "" {
		snapshot.Error = output.Error
		snapshot.OutputTail = tailString(output.Output, 8192)
	}
	if len(candidates) == 0 {
		if snapshot.Error == "" {
			snapshot.Error = "no ltx entries parsed"
			snapshot.OutputTail = tailString(output.Output, 8192)
		}
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

func parseRestorePlanCandidates(output string) []restorePlanCandidate {
	candidates := make([]restorePlanCandidate, 0)
	for _, line := range nonEmptyLines(output) {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		level, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		minTXID, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			continue
		}
		maxTXID, err := strconv.ParseUint(fields[2], 16, 64)
		if err != nil {
			continue
		}
		sizeBytes, _ := strconv.ParseInt(fields[3], 10, 64)
		candidates = append(candidates, restorePlanCandidate{
			level:     level,
			minTXID:   minTXID,
			maxTXID:   maxTXID,
			sizeBytes: sizeBytes,
			createdAt: fields[4],
		})
	}
	return candidates
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
	defer file.Close()
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

func objectLevel(objectPath string) string {
	for _, part := range strings.Split(objectPath, "/") {
		if strings.HasPrefix(part, "L") || strings.HasPrefix(part, "level") {
			return part
		}
		if len(part) == 4 {
			if _, err := strconv.Atoi(part); err == nil {
				return part
			}
		}
	}
	return "unknown"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
