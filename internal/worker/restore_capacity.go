package worker

import (
	"fmt"
	"path/filepath"
	"syscall"
)

const (
	// restoreCapacityMarginPercent is the headroom added to the source database
	// plus WAL size when estimating the space a restore needs: Litestream writes
	// the full database to <output>.tmp before renaming it into place.
	restoreCapacityMarginPercent = 10
	restoreCapacityMinimumBytes  = 64 << 20
)

// restoreFreeBytes reports the bytes available to the harness on the file
// system holding dir. Tests override it.
var restoreFreeBytes = func(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

func restoreNeededBytes(sourceSize int64) uint64 {
	if sourceSize < 0 {
		sourceSize = 0
	}
	needed := uint64(sourceSize) + uint64(sourceSize)*restoreCapacityMarginPercent/100
	return max(needed, restoreCapacityMinimumBytes)
}

// checkRestoreCapacity refuses to start a restore whose output cannot fit on
// the volume. A shortfall is a harness capacity problem, not evidence about the
// build under test, so it surfaces as an inconclusive verification.
func checkRestoreCapacity(restoredPath string, sourceSize int64) error {
	needed := restoreNeededBytes(sourceSize)
	free, err := restoreFreeBytes(filepath.Dir(restoredPath))
	if err != nil {
		return nil
	}
	if free >= needed {
		return nil
	}
	return &restoreCapacityInconclusiveError{
		err:         fmt.Errorf("restore skipped: harness capacity: restore output %s needs %d bytes but only %d bytes are free", restoredPath, needed, free),
		neededBytes: needed,
		freeBytes:   free,
	}
}
