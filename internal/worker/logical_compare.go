package worker

import (
	"context"
	"fmt"
)

func CompareLogicalDatabases(ctx context.Context, sourcePath, restoredPath string) error {
	limits := DefaultConfig().logicalLimits()
	source, err := readLogicalSnapshot(ctx, sourcePath, limits)
	if err != nil {
		return fmt.Errorf("%s source: %w", logicalValidatorVersion, err)
	}
	restored, err := readLogicalSnapshot(ctx, restoredPath, limits)
	if err != nil {
		return fmt.Errorf("%s restored: %w", logicalValidatorVersion, err)
	}
	return compareLogicalSnapshots(source, restored)
}
