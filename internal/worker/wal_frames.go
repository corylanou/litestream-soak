package worker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	walHeaderSize      = 32
	walFrameHeaderSize = 24
	walMagicBigEndian  = 0x377f0683
	walMagicLittle     = 0x377f0682
	ltxHeaderSize      = 100
)

// walHeader is the first 32 bytes of a SQLite WAL file. Salt-1 is incremented
// by one on every WAL reset and salt-2 is re-randomized, so the salt-1
// difference between two headers is the number of resets between them.
type walHeader struct {
	magic         uint32
	version       uint32
	pageSize      uint32
	checkpointSeq uint32
	salt1         uint32
	salt2         uint32
	valid         bool
}

func parseWALHeader(raw []byte) walHeader {
	if len(raw) < walHeaderSize {
		return walHeader{}
	}
	header := walHeader{
		magic:         binary.BigEndian.Uint32(raw[0:4]),
		version:       binary.BigEndian.Uint32(raw[4:8]),
		pageSize:      binary.BigEndian.Uint32(raw[8:12]),
		checkpointSeq: binary.BigEndian.Uint32(raw[12:16]),
		salt1:         binary.BigEndian.Uint32(raw[16:20]),
		salt2:         binary.BigEndian.Uint32(raw[20:24]),
	}
	header.valid = (header.magic == walMagicBigEndian || header.magic == walMagicLittle) && header.version == 3007000 && header.pageSize >= 512
	return header
}

// walChecksum is SQLite's WAL checksum: a pair of 32-bit sums over 8-byte
// chunks, seeded with the previous pair, reading the words in the byte order
// the WAL magic selects.
func walChecksum(bigEndian bool, s0, s1 uint32, data []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(data); i += 8 {
		var x0, x1 uint32
		if bigEndian {
			x0, x1 = binary.BigEndian.Uint32(data[i:]), binary.BigEndian.Uint32(data[i+4:])
		} else {
			x0, x1 = binary.LittleEndian.Uint32(data[i:]), binary.LittleEndian.Uint32(data[i+4:])
		}
		s0 += x0 + s1
		s1 += x1 + s0
	}
	return s0, s1
}

// walGeneration is what the WAL file holds right now: its header, the
// physical size (which SQLite keeps across resets when no journal_size_limit
// is set, so stale frames of earlier generations survive at the tail), and
// how many committed frames belong to the current generation, that is, carry
// the header's salts and a valid checksum chain up to the last commit frame.
type walGeneration struct {
	header          walHeader
	fileBytes       int64
	frameSize       int64
	frameSlots      int64
	committedFrames int64
}

// generationEnd is the byte offset just past the current generation's last
// committed frame.
func (g walGeneration) generationEnd() int64 {
	return walHeaderSize + g.committedFrames*g.frameSize
}

func readWALGeneration(path string) (walGeneration, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return walGeneration{}, nil
		}
		return walGeneration{}, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return walGeneration{}, err
	}
	return walkWALGeneration(file, info.Size())
}

func walkWALGeneration(r io.ReaderAt, fileBytes int64) (walGeneration, error) {
	generation := walGeneration{fileBytes: fileBytes}
	raw := make([]byte, walHeaderSize)
	if n, err := r.ReadAt(raw, 0); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return generation, err
	} else if n < walHeaderSize {
		return generation, nil
	}
	generation.header = parseWALHeader(raw)
	if !generation.header.valid {
		return generation, nil
	}
	bigEndian := generation.header.magic == walMagicBigEndian
	s0, s1 := walChecksum(bigEndian, 0, 0, raw[:24])
	if s0 != binary.BigEndian.Uint32(raw[24:28]) || s1 != binary.BigEndian.Uint32(raw[28:32]) {
		generation.header.valid = false
		return generation, nil
	}
	generation.frameSize = int64(generation.header.pageSize) + walFrameHeaderSize
	generation.frameSlots = (fileBytes - walHeaderSize) / generation.frameSize
	frame := make([]byte, generation.frameSize)
	for i := int64(0); i < generation.frameSlots; i++ {
		if _, err := r.ReadAt(frame, walHeaderSize+i*generation.frameSize); err != nil {
			return generation, err
		}
		if binary.BigEndian.Uint32(frame[8:12]) != generation.header.salt1 || binary.BigEndian.Uint32(frame[12:16]) != generation.header.salt2 {
			break
		}
		s0, s1 = walChecksum(bigEndian, s0, s1, frame[:8])
		s0, s1 = walChecksum(bigEndian, s0, s1, frame[walFrameHeaderSize:])
		if s0 != binary.BigEndian.Uint32(frame[16:20]) || s1 != binary.BigEndian.Uint32(frame[20:24]) {
			break
		}
		if binary.BigEndian.Uint32(frame[4:8]) != 0 {
			generation.committedFrames = i + 1
		}
	}
	return generation, nil
}

// litestreamSavedPosition is the WAL position Litestream last synced, read
// from the header of its newest local L0 LTX file: WALOffset+WALSize is the
// end of the WAL content that file covers (what Litestream resumes from),
// and the salts name the WAL generation it was read from.
type litestreamSavedPosition struct {
	walOffsetEnd int64
	salt1        uint32
	salt2        uint32
	maxTXID      uint64
	file         string
	found        bool
}

func readLitestreamSavedPosition(dbPath string) (litestreamSavedPosition, error) {
	dir := filepath.Join(litestreamStateDir(dbPath), "ltx", "0")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return litestreamSavedPosition{}, nil
		}
		return litestreamSavedPosition{}, err
	}
	var newest litestreamSavedPosition
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for i := len(names) - 1; i >= 0; i-- {
		_, maxTXID, ok := parseLTXFilename(names[i])
		if !ok {
			continue
		}
		position, err := readLTXWALPosition(filepath.Join(dir, names[i]))
		if err != nil {
			return litestreamSavedPosition{}, fmt.Errorf("read %s: %w", names[i], err)
		}
		position.maxTXID = maxTXID
		position.file = names[i]
		newest = position
		break
	}
	return newest, nil
}

func readLTXWALPosition(path string) (litestreamSavedPosition, error) {
	file, err := os.Open(path)
	if err != nil {
		return litestreamSavedPosition{}, err
	}
	defer func() { _ = file.Close() }()
	raw := make([]byte, ltxHeaderSize)
	if _, err := io.ReadFull(file, raw); err != nil {
		return litestreamSavedPosition{}, err
	}
	return parseLTXWALPosition(raw)
}

// parseLTXWALPosition reads the WAL fields of an LTX header (ltx v0.5.2
// layout: magic at 0, WALOffset at 48, WALSize at 56, WALSalt1 at 64,
// WALSalt2 at 68).
func parseLTXWALPosition(raw []byte) (litestreamSavedPosition, error) {
	if len(raw) < ltxHeaderSize || string(raw[0:4]) != "LTX1" {
		return litestreamSavedPosition{}, errors.New("not an ltx header")
	}
	offset := int64(binary.BigEndian.Uint64(raw[48:]))
	size := int64(binary.BigEndian.Uint64(raw[56:]))
	return litestreamSavedPosition{
		walOffsetEnd: offset + size,
		salt1:        binary.BigEndian.Uint32(raw[64:]),
		salt2:        binary.BigEndian.Uint32(raw[68:]),
		found:        true,
	}, nil
}
