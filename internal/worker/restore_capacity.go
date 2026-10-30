package worker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	// restoreCapacityMarginPercent is the headroom added to the estimated
	// restore output when checking free space: Litestream writes the whole
	// database to <output>.tmp before renaming it into place.
	restoreCapacityMarginPercent = 10
	restoreCapacityMinimumBytes  = 64 << 20

	sqliteHeaderSize   = 100
	sqliteWALHeaderLen = 32
	sqliteWALFrameLen  = 24
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

// restoreOutputPaths lists the files an interrupted restore can leave behind
// around the restored target: its temporary output, the pre-upgrade restore
// target and its temporary output, and their SQLite side files. The target
// itself is cleaned at the start and end of every cycle, so the preflight
// leaves it alone and reclaims only what a killed restore abandoned.
func restoreOutputPaths(restoredPath string) []string {
	paths := []string{restoredPath + "-journal"}
	for _, base := range []string{restoredPath + ".tmp", restoredPath + ".pre", restoredPath + ".pre.tmp"} {
		paths = append(paths, base, base+"-wal", base+"-shm", base+"-journal")
	}
	return paths
}

func removeAbandonedRestoreOutput(restoredPath string) error {
	var errs []error
	for _, path := range restoreOutputPaths(restoredPath) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// estimateRestoreOutputBytes estimates the size of the database a restore of
// sourcePath produces. The WAL may rewrite the same pages many times, so the
// physical WAL size overstates the result; the logical size is the largest of
// the main file, the page count the database header declares, and the
// database size the newest WAL commit frame records, each in whole pages.
func estimateRestoreOutputBytes(sourcePath string) int64 {
	dbSize := fileSize(sourcePath)
	estimate := dbSize
	pageSize, headerPages := sqliteHeaderGeometry(sourcePath)
	if pageSize > 0 && headerPages > 0 {
		estimate = max(estimate, headerPages*pageSize)
	}
	if committed := sqliteWALCommittedPages(sourcePath+"-wal", pageSize); committed > 0 && pageSize > 0 {
		estimate = max(estimate, committed*pageSize)
	}
	return estimate
}

func sqliteHeaderGeometry(path string) (pageSize, pages int64) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, sqliteHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil || string(header[:15]) != "SQLite format 3" {
		return 0, 0
	}
	pageSize = int64(binary.BigEndian.Uint16(header[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return 0, 0
	}
	return pageSize, int64(binary.BigEndian.Uint32(header[28:32]))
}

// sqliteWALCommittedPages returns the database size in pages recorded by the
// newest commit frame in the WAL, which is the logical size of the database
// once that WAL is applied.
func sqliteWALCommittedPages(walPath string, pageSize int64) int64 {
	f, err := os.Open(walPath)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	header := make([]byte, sqliteWALHeaderLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0
	}
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != 0x377f0682 && magic != 0x377f0683 {
		return 0
	}
	walPageSize := int64(binary.BigEndian.Uint32(header[8:12]))
	if walPageSize <= 0 || (pageSize > 0 && walPageSize != pageSize) {
		return 0
	}
	frame := make([]byte, sqliteWALFrameLen)
	var committed int64
	for offset := int64(sqliteWALHeaderLen); ; offset += sqliteWALFrameLen + walPageSize {
		if _, err := f.ReadAt(frame, offset); err != nil {
			break
		}
		if size := int64(binary.BigEndian.Uint32(frame[4:8])); size > 0 {
			committed = size
		}
	}
	return committed
}

func restoreNeededBytes(outputBytes int64) uint64 {
	if outputBytes < 0 {
		outputBytes = 0
	}
	needed := uint64(outputBytes) + uint64(outputBytes)*restoreCapacityMarginPercent/100
	return max(needed, restoreCapacityMinimumBytes)
}

// checkRestoreCapacity clears abandoned restore output, then refuses to start a
// restore whose output cannot fit on the volume. A shortfall is a harness
// capacity problem, not evidence about the build under test, so it surfaces as
// an inconclusive verification.
func checkRestoreCapacity(restoredPath, sourcePath string) error {
	if err := removeAbandonedRestoreOutput(restoredPath); err != nil {
		return fmt.Errorf("clean abandoned restore output: %w", err)
	}
	needed := restoreNeededBytes(estimateRestoreOutputBytes(sourcePath))
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

func restoreCapacityExhausted(err error, restoredPath, sourcePath string) *restoreCapacityInconclusiveError {
	free, _ := restoreFreeBytes(filepath.Dir(restoredPath))
	return &restoreCapacityInconclusiveError{err: err, neededBytes: restoreNeededBytes(estimateRestoreOutputBytes(sourcePath)), freeBytes: free}
}
