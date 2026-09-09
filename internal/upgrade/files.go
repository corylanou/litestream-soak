package upgrade

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func copyState(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("fixture source must be a directory, not a symlink")
	}
	sourcePath, err := filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	sourcePath, err = filepath.Abs(sourcePath)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(destination))
	if err != nil {
		return err
	}
	destinationPath, err := filepath.Abs(filepath.Join(parent, filepath.Base(destination)))
	if err != nil {
		return err
	}
	if sourcePath == destinationPath || strings.HasPrefix(destinationPath, sourcePath+string(os.PathSeparator)) || strings.HasPrefix(sourcePath, destinationPath+string(os.PathSeparator)) {
		return errors.New("fixture source and destination overlap")
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return err
	}
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.Mkdir(target, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("refuse non-regular fixture entry: %s", relative)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func verifyPin(binary Binary) error {
	hash, err := fileHash(binary.Path)
	if err != nil {
		return err
	}
	if hash != binary.SHA256 {
		return errors.New("binary bytes changed from expected pin")
	}
	return nil
}
