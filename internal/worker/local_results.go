package worker

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/corylanou/litestream-soak/internal/workload"
)

type localResult struct {
	RecordedAt  time.Time          `json:"recorded_at"`
	Backend     string             `json:"backend"`
	ReplicaType string             `json:"replica_type"`
	Profile     string             `json:"profile"`
	Databases   int                `json:"databases"`
	Result      VerificationResult `json:"result"`
}

func (r *Runner) recordLocalResult(result VerificationResult) {
	if r.cfg.LocalResultsPath == "" {
		return
	}
	body, err := json.Marshal(localResult{
		RecordedAt:  time.Now().UTC(),
		Backend:     workload.NormalizeReplicaBackend(r.cfg.ReplicaBackend),
		ReplicaType: r.cfg.ReplicaType,
		Profile:     r.cfg.ProfileName,
		Databases:   max(r.cfg.NumDatabases, 1),
		Result:      result,
	})
	if err == nil {
		err = os.MkdirAll(filepath.Dir(r.cfg.LocalResultsPath), 0755)
	}
	if err == nil {
		err = appendLine(r.cfg.LocalResultsPath, body)
	}
	if err != nil {
		slog.Warn("Record local verification result failed", "error", err)
	}
}

func appendLine(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(body, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
