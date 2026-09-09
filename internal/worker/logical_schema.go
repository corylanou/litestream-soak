package worker

import (
	"context"
	"fmt"
)

func CompareLogicalSchemas(ctx context.Context, sourcePath, restoredPath string) error {
	limits := DefaultConfig().logicalLimits()
	source, err := readLogicalSnapshotMode(ctx, sourcePath, limits, true)
	if err != nil {
		return fmt.Errorf("%s source schema: %w", logicalValidatorVersion, err)
	}
	restored, err := readLogicalSnapshotMode(ctx, restoredPath, limits, true)
	if err != nil {
		return fmt.Errorf("%s restored schema: %w", logicalValidatorVersion, err)
	}
	return compareLogicalSnapshots(source, restored)
}
