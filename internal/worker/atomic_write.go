package worker

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeFileAtomic replaces path with body without ever exposing a truncated
// file: the body is written and fsynced to a sibling temp file, renamed over
// path, and the directory is fsynced. A failed write leaves any previous file
// untouched and removes the temp file.
func writeFileAtomic(path string, body []byte) error {
	tmp := path + ".tmp"
	if err := writeFileDurable(tmp, body); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", filepath.Base(tmp), err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit %s: %w", filepath.Base(path), err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync %s: %w", filepath.Dir(path), err)
	}
	return nil
}
