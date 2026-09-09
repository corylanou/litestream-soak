package upgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
)

type fixtureSeal struct {
	Age            Age               `json:"age"`
	BaselineSHA256 string            `json:"baseline_sha256"`
	Files          map[string]string `json:"files"`
	Failure        string            `json:"failure,omitempty"`
}

func inventory(state string) (map[string]string, error) {
	files := make(map[string]string)
	err := filepath.WalkDir(state, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("fixture contains non-regular entry")
		}
		relative, err := filepath.Rel(state, path)
		if err != nil {
			return err
		}
		hash, err := fileHash(path)
		if err != nil {
			return err
		}
		files[relative] = hash
		return nil
	})
	return files, err
}

func sealFixture(state string, age Age, baseline, failure string) error {
	files, err := inventory(state)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(fixtureSeal{Age: age, BaselineSHA256: baseline, Files: files, Failure: failure}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(state+".json", data, 0600)
}

func importFixture(source, destination, baseline string) (Age, error) {
	data, err := os.ReadFile(source + ".json")
	if err != nil {
		return Age{}, err
	}
	var seal fixtureSeal
	if err := json.Unmarshal(data, &seal); err != nil {
		return Age{}, err
	}
	if seal.BaselineSHA256 != baseline {
		return seal.Age, errors.New("fixture baseline binary pin differs")
	}
	if len(seal.Files) == 0 {
		return seal.Age, errors.New("fixture has no sealed contents")
	}
	if err := copyState(source, destination); err != nil {
		return seal.Age, err
	}
	files, err := inventory(destination)
	if err != nil {
		return seal.Age, err
	}
	if !reflect.DeepEqual(files, seal.Files) {
		return seal.Age, errors.New("fixture contents differ from quiescent seal")
	}
	if err := sealFixture(destination, seal.Age, baseline, seal.Failure); err != nil {
		return seal.Age, err
	}
	if seal.Failure != "" {
		return seal.Age, fmt.Errorf("retained fixture failure: %s", seal.Failure)
	}
	return seal.Age, nil
}
