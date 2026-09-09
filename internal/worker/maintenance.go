package worker

import "github.com/corylanou/litestream-soak/internal/reporting"

func (b *lineBuffer) MaintenanceEvidence() reporting.MaintenanceEvidence {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maintenance
}
