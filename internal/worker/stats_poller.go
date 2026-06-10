package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

	snapshotMu    sync.Mutex
	snapshot      runtimeSnapshot
	lastLocalPoll time.Time
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
	if info, err := os.Stat(p.cfg.DBPath); err == nil {
		SetDBSize(info.Size())
		p.setDBSize(info.Size())
	}
	if info, err := os.Stat(p.cfg.DBPath + "-wal"); err == nil {
		SetWALSize(info.Size())
		p.setWALSize(info.Size())
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
	txid, err := p.pollTXID(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	uptimeSeconds, err := p.pollInfo(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}
	dbStatus, lastSyncAgeSeconds, err := p.pollList(client)
	if err != nil {
		return reporting.RuntimePayload{}, err
	}

	return reporting.RuntimePayload{
		DBTXID:                    txid,
		DBStatus:                  dbStatus,
		LastSyncAgeSeconds:        lastSyncAgeSeconds,
		LitestreamUptimeSeconds:   uptimeSeconds,
		SnapshotCollectedAt:       collectedAt,
		LitestreamSnapshotHealthy: true,
	}, nil
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

func (p *statsPoller) pollList(client *http.Client) (string, float64, error) {
	resp, err := client.Get("http://localhost/list")
	if err != nil {
		return "", 0, fmt.Errorf("read database list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Databases []struct {
			Status         string     `json:"status"`
			TXID           *uint64    `json:"txid"`
			ReplicatedTXID *uint64    `json:"replicated_txid"`
			LastSyncAt     *time.Time `json:"last_sync_at,omitempty"`
		} `json:"databases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode database list: %w", err)
	}
	if len(result.Databases) == 0 {
		return "", 0, errors.New("database list was empty")
	}

	db := result.Databases[0]
	if db.TXID != nil && db.ReplicatedTXID != nil {
		lag := uint64(0)
		if *db.TXID > *db.ReplicatedTXID {
			lag = *db.TXID - *db.ReplicatedTXID
		}
		SetReplicatedTXID(float64(*db.ReplicatedTXID))
		SetReplicationLag(float64(lag))
	}
	age := 0.0
	if db.LastSyncAt != nil {
		age = time.Since(*db.LastSyncAt).Seconds()
	}
	return db.Status, age, nil
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
	p.snapshot.DBStatus = snapshot.DBStatus
	p.snapshot.LastSyncAgeSeconds = snapshot.LastSyncAgeSeconds
	p.snapshot.LitestreamUptimeSeconds = snapshot.LitestreamUptimeSeconds
	p.snapshot.SnapshotCollectedAt = snapshot.SnapshotCollectedAt
	p.snapshot.LitestreamSnapshotHealthy = snapshot.LitestreamSnapshotHealthy
	p.snapshot.LitestreamSnapshotError = snapshot.LitestreamSnapshotError
	SetDBTXID(float64(snapshot.DBTXID))
	SetDBStatus(snapshot.DBStatus)
	SetLastSyncAge(snapshot.LastSyncAgeSeconds)
	SetLitestreamUptime(snapshot.LitestreamUptimeSeconds)
	SetLitestreamSnapshotHealthy(snapshot.LitestreamSnapshotHealthy)
}

func (p *statsPoller) setLitestreamSnapshotFailure(collectedAt time.Time, err error) {
	p.setLitestreamSnapshot(reporting.RuntimePayload{
		DBTXID:                    0,
		DBStatus:                  "unknown",
		LastSyncAgeSeconds:        0,
		LitestreamUptimeSeconds:   0,
		SnapshotCollectedAt:       collectedAt,
		LitestreamSnapshotHealthy: false,
		LitestreamSnapshotError:   err.Error(),
	})
}
