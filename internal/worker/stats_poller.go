package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/corylanou/litestream-soak/internal/reporting"
)

type runtimeSnapshot struct {
	reporting.RuntimePayload
}

type statsPoller struct {
	cfg *Config

	snapshotMu     sync.Mutex
	snapshot       runtimeSnapshot
	lastLocalPoll  time.Time
	litestreamPID  func() int
	s3ListRequests func() int64
	prevAllocBytes float64
	prevAllocAt    time.Time
}

func newStatsPoller(cfg *Config) statsPoller {
	return statsPoller{
		cfg: cfg,
		snapshot: runtimeSnapshot{
			RuntimePayload: reporting.RuntimePayload{
				DBStatus:                "unknown",
				LitestreamSnapshotError: "litestream stats not collected yet",
			},
		},
	}
}

func (p *statsPoller) pollDBStats() {
	if p.cfg.ManyDBEnabled() {
		count, dbBytes, walBytes := p.manyDBFileStats()
		SetDBSize(dbBytes)
		SetWALSize(walBytes)
		SetDBAggregateStats(count, dbBytes, walBytes)
		p.setDBSize(dbBytes)
		p.setWALSize(walBytes)
		p.setDBAggregateStats(count, dbBytes, walBytes)
	} else {
		count := 0
		dbBytes := int64(0)
		walBytes := int64(0)
		if info, err := os.Stat(p.cfg.DBPath); err == nil {
			count = 1
			dbBytes = info.Size()
			SetDBSize(info.Size())
			p.setDBSize(info.Size())
		}
		if info, err := os.Stat(p.cfg.DBPath + "-wal"); err == nil {
			walBytes = info.Size()
			SetWALSize(info.Size())
			p.setWALSize(info.Size())
		}
		SetDBAggregateStats(count, dbBytes, walBytes)
		p.setDBAggregateStats(count, dbBytes, walBytes)
	}
	p.pollDataDiskStats()
	if time.Since(p.lastLocalPoll) >= time.Minute {
		p.lastLocalPoll = time.Now()
		p.pollLitestreamLocalState()
	}

	client := p.ipcClient()
	defer client.CloseIdleConnections()
	snapshot, err := p.collectLitestreamRuntime(client, time.Now().UTC())
	if err != nil {
		p.setLitestreamSnapshotFailure(time.Now().UTC(), err)
		return
	}
	p.setLitestreamSnapshot(snapshot)
}

func (p *statsPoller) ipcClient() *http.Client {
	return newIPCClient(p.cfg.SocketPath, 5*time.Second)
}

func newIPCClient(socketPath string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: timeout,
	}
}

func (p *statsPoller) collectLitestreamRuntime(client *http.Client, collectedAt time.Time) (reporting.RuntimePayload, error) {
	litestreamMetrics := p.pollLitestreamMetrics(client)
	listRequests := p.currentS3ListRequests()
	SetS3ListRequests(listRequests)
	allocRate := 0.0
	if litestreamMetrics.MemStatsPresent {
		allocRate = p.deriveAllocRate(collectedAt, litestreamMetrics.AllocBytesTotal)
		SetLitestreamMemStats(litestreamMetrics.HeapInuseBytes, litestreamMetrics.StackInuseBytes, litestreamMetrics.AllocBytesTotal, allocRate)
	}
	if p.cfg.ManyDBEnabled() {
		uptimeSeconds, err := p.pollInfo(client)
		if err != nil {
			return reporting.RuntimePayload{}, err
		}
		databases, err := p.pollListDatabases(client)
		if err != nil {
			return reporting.RuntimePayload{}, err
		}
		snapshot := p.aggregateManyDBRuntime(databases, uptimeSeconds, collectedAt)
		process := collectProcessStats(p.currentLitestreamPID())
		process.LitestreamGoroutines = p.pollLitestreamGoroutineCount(client)
		snapshot.LitestreamDiskFullMetricPresent = litestreamMetrics.DiskFullPresent
		snapshot.LitestreamDiskFull = litestreamMetrics.DiskFull
		snapshot.LitestreamRSSBytes = process.LitestreamRSSBytes
		snapshot.LitestreamCPUSecondsTotal = process.LitestreamCPUSecondsTotal
		snapshot.LitestreamGoroutines = process.LitestreamGoroutines
		snapshot.LitestreamFDs = process.LitestreamFDs
		snapshot.WorkerRSSBytes = process.WorkerRSSBytes
		snapshot.WorkerFDs = process.WorkerFDs
		snapshot.S3ListRequestsTotal = listRequests
		if litestreamMetrics.MemStatsPresent {
			snapshot.LitestreamHeapInuseBytes = uint64(litestreamMetrics.HeapInuseBytes)
			snapshot.LitestreamStackInuseBytes = uint64(litestreamMetrics.StackInuseBytes)
			snapshot.LitestreamAllocBytesTotal = litestreamMetrics.AllocBytesTotal
			snapshot.LitestreamAllocRateBytesPerSec = allocRate
		}
		SetProcessStats(process)
		return snapshot, nil
	}

	txid, err := p.pollTXID(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	uptimeSeconds, err := p.pollInfo(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	dbStatus, replicatedTXID, replicationLag, lastSyncAgeSeconds, err := p.pollList(client, txid)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	process := collectProcessStats(p.currentLitestreamPID())
	process.LitestreamGoroutines = p.pollLitestreamGoroutineCount(client)
	SetProcessStats(process)

	snapshot := reporting.RuntimePayload{
		DBTXID:                          txid,
		ReplicatedTXID:                  replicatedTXID,
		DBStatus:                        dbStatus,
		LastSyncAgeSeconds:              lastSyncAgeSeconds,
		LastSyncAgeP50Seconds:           lastSyncAgeSeconds,
		LastSyncAgeP95Seconds:           lastSyncAgeSeconds,
		LastSyncAgeMaxSeconds:           lastSyncAgeSeconds,
		ReplicationLagP95:               replicationLag,
		ReplicationLagMax:               replicationLag,
		LitestreamDiskFullMetricPresent: litestreamMetrics.DiskFullPresent,
		LitestreamDiskFull:              litestreamMetrics.DiskFull,
		LitestreamRSSBytes:              process.LitestreamRSSBytes,
		LitestreamCPUSecondsTotal:       process.LitestreamCPUSecondsTotal,
		LitestreamGoroutines:            process.LitestreamGoroutines,
		LitestreamFDs:                   process.LitestreamFDs,
		WorkerRSSBytes:                  process.WorkerRSSBytes,
		WorkerFDs:                       process.WorkerFDs,
		LitestreamUptimeSeconds:         uptimeSeconds,
		S3ListRequestsTotal:             listRequests,
		SnapshotCollectedAt:             collectedAt,
		LitestreamSnapshotHealthy:       true,
	}
	if litestreamMetrics.MemStatsPresent {
		snapshot.LitestreamHeapInuseBytes = uint64(litestreamMetrics.HeapInuseBytes)
		snapshot.LitestreamStackInuseBytes = uint64(litestreamMetrics.StackInuseBytes)
		snapshot.LitestreamAllocBytesTotal = litestreamMetrics.AllocBytesTotal
		snapshot.LitestreamAllocRateBytesPerSec = allocRate
	}
	return snapshot, nil
}

func (p *statsPoller) pollTXID(client *http.Client) (uint64, error) {
	resp, err := client.Get("http://localhost/txid?path=" + p.cfg.DBPath)
	if err != nil {
		return 0, fmt.Errorf("read txid: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		TXID uint64 `json:"txid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode txid: %w", err)
	}
	return result.TXID, nil
}

func (p *statsPoller) pollInfo(client *http.Client) (float64, error) {
	resp, err := client.Get("http://localhost/info")
	if err != nil {
		return 0, fmt.Errorf("read info: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		UptimeSeconds int64 `json:"uptime_seconds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode info: %w", err)
	}
	return float64(result.UptimeSeconds), nil
}

type litestreamListDatabase struct {
	Path           string     `json:"path"`
	Status         string     `json:"status"`
	TXID           *uint64    `json:"txid"`
	ReplicatedTXID *uint64    `json:"replicated_txid"`
	LastSyncAt     *time.Time `json:"last_sync_at,omitempty"`
}

func (p *statsPoller) pollListDatabases(client *http.Client) ([]litestreamListDatabase, error) {
	resp, err := client.Get("http://localhost/list")
	if err != nil {
		return nil, fmt.Errorf("read database list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Databases []litestreamListDatabase `json:"databases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode database list: %w", err)
	}
	if len(result.Databases) == 0 {
		return nil, errors.New("database list was empty")
	}
	return result.Databases, nil
}

func (p *statsPoller) pollList(client *http.Client, localTXID uint64) (string, uint64, uint64, float64, error) {
	databases, err := p.pollListDatabases(client)
	if err != nil {
		return "", 0, 0, 0, err
	}

	db := databases[0]
	replicatedTXID := uint64(0)
	lag := uint64(0)
	if db.ReplicatedTXID != nil {
		lagTXID := localTXID
		if db.TXID != nil {
			lagTXID = *db.TXID
			if lagTXID > 0 && localTXID > lagTXID {
				lagTXID = localTXID
			}
		}
		if lagTXID > *db.ReplicatedTXID {
			lag = lagTXID - *db.ReplicatedTXID
		}
		replicatedTXID = *db.ReplicatedTXID
		SetReplicatedTXID(float64(*db.ReplicatedTXID))
		SetReplicationLag(float64(lag))
	}
	age := 0.0
	if db.LastSyncAt != nil {
		age = time.Since(*db.LastSyncAt).Seconds()
	}
	return db.Status, replicatedTXID, lag, age, nil
}

func (p *statsPoller) pollLitestreamMetrics(client *http.Client) litestreamMetricsSnapshot {
	resp, err := client.Get("http://localhost/metrics")
	if err != nil {
		return litestreamMetricsSnapshot{}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return litestreamMetricsSnapshot{}
	}
	dbPath := p.cfg.DBPath
	if p.cfg.ManyDBEnabled() {
		dbPath = ""
	}
	snapshot, err := parseLitestreamMetrics(resp.Body, dbPath)
	if err != nil {
		return litestreamMetricsSnapshot{}
	}
	return snapshot
}

func (p *statsPoller) currentS3ListRequests() uint64 {
	if p.s3ListRequests == nil {
		return 0
	}
	count := p.s3ListRequests()
	if count < 0 {
		return 0
	}
	return uint64(count)
}

func (p *statsPoller) deriveAllocRate(now time.Time, alloc float64) float64 {
	if p.prevAllocAt.IsZero() || alloc < p.prevAllocBytes {
		p.prevAllocBytes = alloc
		p.prevAllocAt = now
		return 0
	}
	elapsed := now.Sub(p.prevAllocAt).Seconds()
	if elapsed <= 0 {
		return 0
	}
	rate := (alloc - p.prevAllocBytes) / elapsed
	p.prevAllocBytes = alloc
	p.prevAllocAt = now
	return rate
}

func (p *statsPoller) aggregateManyDBRuntime(databases []litestreamListDatabase, uptimeSeconds float64, collectedAt time.Time) reporting.RuntimePayload {
	lags := make([]uint64, 0, len(databases))
	ages := make([]float64, 0, len(databases))
	maxTXID := uint64(0)
	maxReplicatedTXID := uint64(0)
	overThreshold := 0
	status := ""
	for _, db := range databases {
		if status == "" {
			status = db.Status
		} else if status != db.Status {
			status = "mixed"
		}
		txid := uint64(0)
		if db.TXID != nil {
			txid = *db.TXID
			if txid > maxTXID {
				maxTXID = txid
			}
		}
		replicatedTXID := uint64(0)
		if db.ReplicatedTXID != nil {
			replicatedTXID = *db.ReplicatedTXID
			if replicatedTXID > maxReplicatedTXID {
				maxReplicatedTXID = replicatedTXID
			}
		}
		lag := uint64(0)
		if txid > replicatedTXID {
			lag = txid - replicatedTXID
		}
		if lag > p.cfg.ReplicationLagThreshold {
			overThreshold++
		}
		lags = append(lags, lag)

		age := 0.0
		if db.LastSyncAt != nil {
			age = collectedAt.Sub(*db.LastSyncAt).Seconds()
			if age < 0 {
				age = 0
			}
		}
		ages = append(ages, age)
	}
	if status == "" {
		status = "unknown"
	}
	lagP95 := percentileUint64(lags, 95)
	lagMax := percentileUint64(lags, 100)
	ageP50 := percentileFloat64(ages, 50)
	ageP95 := percentileFloat64(ages, 95)
	ageMax := percentileFloat64(ages, 100)

	SetDBTXID(float64(maxTXID))
	SetReplicatedTXID(float64(maxReplicatedTXID))
	SetReplicationLag(float64(lagMax))
	SetReplicationLagAggregates(lagP95, lagMax, overThreshold)
	SetLastSyncAge(ageMax)
	SetLastSyncAgeAggregates(ageP50, ageP95, ageMax)
	SetDBStatus(status)

	return reporting.RuntimePayload{
		DBCount:                     len(databases),
		DBTXID:                      maxTXID,
		DBStatus:                    status,
		LastSyncAgeSeconds:          ageMax,
		LastSyncAgeP50Seconds:       ageP50,
		LastSyncAgeP95Seconds:       ageP95,
		LastSyncAgeMaxSeconds:       ageMax,
		ReplicationLagP95:           lagP95,
		ReplicationLagMax:           lagMax,
		ReplicationLagOverThreshold: overThreshold,
		LitestreamUptimeSeconds:     uptimeSeconds,
		ReplicatedTXID:              maxReplicatedTXID,
		SnapshotCollectedAt:         collectedAt,
		LitestreamSnapshotHealthy:   true,
	}
}

func (p *statsPoller) currentSnapshot() runtimeSnapshot {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	return p.snapshot
}

func (p *statsPoller) setUptime(seconds float64) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.UptimeSeconds = seconds
}

func (p *statsPoller) setDBSize(bytes int64) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DBSizeBytes = bytes
}

func (p *statsPoller) setWALSize(bytes int64) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.WALSizeBytes = bytes
}

func (p *statsPoller) setDBAggregateStats(count int, dbBytes, walBytes int64) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DBCount = count
	p.snapshot.DBTotalSizeBytes = dbBytes
	p.snapshot.WALTotalSizeBytes = walBytes
}

func (p *statsPoller) manyDBFileStats() (count int, dbBytes int64, walBytes int64) {
	for _, dbPath := range p.cfg.ManyDBPaths() {
		if info, err := os.Stat(dbPath); err == nil {
			count++
			dbBytes += info.Size()
		}
		if info, err := os.Stat(dbPath + "-wal"); err == nil {
			walBytes += info.Size()
		}
	}
	return count, dbBytes, walBytes
}

func (p *statsPoller) pollDataDiskStats() {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(p.cfg.DataDir, &stat); err != nil {
		return
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := total - free
	usedPercent := 0.0
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}

	SetDataDiskStats(total, used, free, usedPercent)
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DataDiskTotalBytes = total
	p.snapshot.DataDiskUsedBytes = used
	p.snapshot.DataDiskFreeBytes = free
	p.snapshot.DataDiskAvailableBytes = available
	p.snapshot.DataDiskUsedPercent = usedPercent
}

func (p *statsPoller) pollLitestreamLocalState() {
	stateDir := litestreamStateDir(p.cfg.DBPath)
	dirBytes := directorySize(stateDir)
	ltxBytes := directorySize(filepath.Join(stateDir, "ltx"))

	SetLitestreamLocalStateSize(dirBytes, ltxBytes)
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.LitestreamDirSizeBytes = dirBytes
	p.snapshot.LitestreamLTXSizeBytes = ltxBytes
}

func litestreamStateDir(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "."+filepath.Base(dbPath)+"-litestream")
}

func directorySize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

func (p *statsPoller) setLitestreamSnapshot(snapshot reporting.RuntimePayload) {
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshot.DBTXID = snapshot.DBTXID
	p.snapshot.ReplicatedTXID = snapshot.ReplicatedTXID
	if snapshot.DBCount > 0 {
		p.snapshot.DBCount = snapshot.DBCount
	}
	if snapshot.DBTotalSizeBytes > 0 {
		p.snapshot.DBTotalSizeBytes = snapshot.DBTotalSizeBytes
	}
	if snapshot.WALTotalSizeBytes > 0 {
		p.snapshot.WALTotalSizeBytes = snapshot.WALTotalSizeBytes
	}
	p.snapshot.DBStatus = snapshot.DBStatus
	p.snapshot.LastSyncAgeSeconds = snapshot.LastSyncAgeSeconds
	p.snapshot.LastSyncAgeP50Seconds = snapshot.LastSyncAgeP50Seconds
	p.snapshot.LastSyncAgeP95Seconds = snapshot.LastSyncAgeP95Seconds
	p.snapshot.LastSyncAgeMaxSeconds = snapshot.LastSyncAgeMaxSeconds
	p.snapshot.ReplicationLagP95 = snapshot.ReplicationLagP95
	p.snapshot.ReplicationLagMax = snapshot.ReplicationLagMax
	p.snapshot.ReplicationLagOverThreshold = snapshot.ReplicationLagOverThreshold
	p.snapshot.LitestreamDiskFullMetricPresent = snapshot.LitestreamDiskFullMetricPresent
	p.snapshot.LitestreamDiskFull = snapshot.LitestreamDiskFull
	p.snapshot.LitestreamRSSBytes = snapshot.LitestreamRSSBytes
	p.snapshot.LitestreamCPUSecondsTotal = snapshot.LitestreamCPUSecondsTotal
	p.snapshot.LitestreamGoroutines = snapshot.LitestreamGoroutines
	p.snapshot.LitestreamFDs = snapshot.LitestreamFDs
	p.snapshot.WorkerRSSBytes = snapshot.WorkerRSSBytes
	p.snapshot.WorkerFDs = snapshot.WorkerFDs
	p.snapshot.DiskPressureNoProgress = snapshot.DiskPressureNoProgress
	p.snapshot.DiskPressureNoProgressSeconds = snapshot.DiskPressureNoProgressSeconds
	p.snapshot.DiskFullSignalObserved = snapshot.DiskFullSignalObserved
	p.snapshot.DiskFullSignalMessage = snapshot.DiskFullSignalMessage
	p.snapshot.DiskFullRecoveryAttempted = snapshot.DiskFullRecoveryAttempted
	p.snapshot.DiskFullRecoveryFreedBytes = snapshot.DiskFullRecoveryFreedBytes
	p.snapshot.DiskFullRecovered = snapshot.DiskFullRecovered
	p.snapshot.DiskFullRecoverySeconds = snapshot.DiskFullRecoverySeconds
	p.snapshot.DiskFullRecoveryWithoutRestart = snapshot.DiskFullRecoveryWithoutRestart
	p.snapshot.LitestreamUptimeSeconds = snapshot.LitestreamUptimeSeconds
	p.snapshot.S3ListRequestsTotal = snapshot.S3ListRequestsTotal
	p.snapshot.LitestreamHeapInuseBytes = snapshot.LitestreamHeapInuseBytes
	p.snapshot.LitestreamStackInuseBytes = snapshot.LitestreamStackInuseBytes
	p.snapshot.LitestreamAllocBytesTotal = snapshot.LitestreamAllocBytesTotal
	p.snapshot.LitestreamAllocRateBytesPerSec = snapshot.LitestreamAllocRateBytesPerSec
	p.snapshot.SnapshotCollectedAt = snapshot.SnapshotCollectedAt
	p.snapshot.LitestreamSnapshotHealthy = snapshot.LitestreamSnapshotHealthy
	p.snapshot.LitestreamSnapshotError = snapshot.LitestreamSnapshotError
	SetDBTXID(float64(snapshot.DBTXID))
	SetDBStatus(snapshot.DBStatus)
	SetLastSyncAge(snapshot.LastSyncAgeSeconds)
	SetLastSyncAgeAggregates(snapshot.LastSyncAgeP50Seconds, snapshot.LastSyncAgeP95Seconds, snapshot.LastSyncAgeMaxSeconds)
	SetReplicationLagAggregates(snapshot.ReplicationLagP95, snapshot.ReplicationLagMax, snapshot.ReplicationLagOverThreshold)
	SetLitestreamUptime(snapshot.LitestreamUptimeSeconds)
	SetLitestreamSnapshotHealthy(snapshot.LitestreamSnapshotHealthy)
}

func (p *statsPoller) setLitestreamSnapshotFailure(collectedAt time.Time, err error) {
	p.setLitestreamSnapshot(reporting.RuntimePayload{
		DBTXID:                      0,
		DBStatus:                    "unknown",
		LastSyncAgeSeconds:          0,
		LastSyncAgeP50Seconds:       0,
		LastSyncAgeP95Seconds:       0,
		LastSyncAgeMaxSeconds:       0,
		ReplicationLagP95:           0,
		ReplicationLagMax:           0,
		ReplicationLagOverThreshold: 0,
		LitestreamUptimeSeconds:     0,
		SnapshotCollectedAt:         collectedAt,
		LitestreamSnapshotHealthy:   false,
		LitestreamSnapshotError:     err.Error(),
	})
}

func (p *statsPoller) currentLitestreamPID() int {
	if p.litestreamPID == nil {
		return 0
	}
	return p.litestreamPID()
}

func (p *statsPoller) pollLitestreamGoroutineCount(client *http.Client) int {
	resp, err := client.Get("http://localhost/debug/pprof/goroutine?debug=1")
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	body, _, err := readLimited(resp.Body, 4096)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "goroutine profile: total ") {
			continue
		}
		value := strings.TrimPrefix(line, "goroutine profile: total ")
		fields := strings.Fields(value)
		if len(fields) == 0 {
			return 0
		}
		count, err := strconv.Atoi(fields[0])
		if err == nil {
			return count
		}
	}
	return 0
}

func percentileUint64(values []uint64, pct float64) uint64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]uint64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := percentileIndex(len(sorted), pct)
	return sorted[idx]
}

func percentileFloat64(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	idx := percentileIndex(len(sorted), pct)
	return sorted[idx]
}

func percentileIndex(length int, pct float64) int {
	if length <= 1 {
		return 0
	}
	if pct <= 0 {
		return 0
	}
	if pct >= 100 {
		return length - 1
	}
	idx := int(math.Ceil((pct/100)*float64(length))) - 1
	if idx < 0 {
		return 0
	}
	if idx >= length {
		return length - 1
	}
	return idx
}
