package worker

import (
	"context"
	"time"
)

func RunPinnedReader(ctx context.Context, dbPath string, hold, pause time.Duration) {
	reader := newPinnedReader(dbPath, hold, pause)
	reader.Start(ctx)
	<-ctx.Done()
	reader.Stop()
}
