package writeaheadlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/multierr"
)

var (
	log = logging.Logger("f3/wal")

	ErrParsingEntry = errors.New("failed to parse log entry")
)

const (
	rotateAt     = 1 << 20 // 1MiB
	walExtension = ".wal.cbor"
)

type Entry interface {
	cbg.CBORMarshaler
	cbg.CBORUnmarshaler

	WALEpoch() uint64
}

type EntryPointer[T any] interface {
	*T
	Entry
}

type Visitor[T any] func(T) (proceed bool)

type WriteAheadLog[T any, PT EntryPointer[T]] struct {
	// pointer type trickery above
	lk sync.Mutex

	path string

	logs   []entriesLog[T]
	active struct {
		file *os.File
		entriesLog[T]
	}
}

type entriesLog[T any] struct {
	logName  string
	maxEpoch uint64
	// entries holds the in-memory cache of WAL entries for fast read access.
	entries []T
}

func Open[T any, PT EntryPointer[T]](directory string) (*WriteAheadLog[T, PT], error) {
	wal := WriteAheadLog[T, PT]{
		path: directory,
	}

	err := wal.hydrate()
	if err != nil {
		return nil, fmt.Errorf("reading the WAL: %w", err)
	}

	return &wal, nil
}

func (wal *WriteAheadLog[T, PT]) All() []T {
	var entries []T
	wal.ForEach(func(entry T) bool {
		entries = append(entries, entry)
		return true
	})
	return entries
}

// ForEach applies the given visitor function to each log entry in the
// WriteAheadLog until either the visitor returns false or there are no more
// entries.
//
// This includes both active and archived log files in the iteration. If a parse
// error occurs while processing an entry (specifically an ErrParsingEntry), the
// error is logged and the remaining entries in that log file are skipped. Other
// unexpected errors will halt the iteration.
func (wal *WriteAheadLog[T, PT]) ForEach(visitor Visitor[T]) {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	allLogs := wal.logs
	if wal.active.file != nil {
		allLogs = append(allLogs, wal.active.entriesLog)
	}
	for _, l := range allLogs {
		for _, entry := range l.entries {
			if !visitor(entry) {
				return
			}
		}
	}
}

func (wal *WriteAheadLog[T, PT]) Append(value T) error {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	if err := wal.maybeRotate(); err != nil {
		return fmt.Errorf("attemting to rotate: %w", err)
	}
	var buf bytes.Buffer

	err := PT(&value).MarshalCBOR(&buf)
	if err != nil {
		return fmt.Errorf("saving value to WAL: %w", err)
	}
	_, err = buf.WriteTo(wal.active.file)
	if err != nil {
		return fmt.Errorf("writing buffer to file: %w", err)
	}

	err = wal.active.file.Sync()
	if err != nil {
		return fmt.Errorf("sycning the file: %w", err)
	}

	wal.active.maxEpoch = max(wal.active.maxEpoch, PT(&value).WALEpoch())
	wal.active.entries = append(wal.active.entries, value)

	return nil
}

// Purge removes files containing entries only older than keepEpoch
// It is not guaranteed to remove all entries older than keepEpoch as it will keep the current
// file.
func (wal *WriteAheadLog[T, PT]) Purge(keepEpoch uint64) error {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	var keptLogFiles []entriesLog[T]
	var err error
	for _, c := range wal.logs {
		if c.maxEpoch < keepEpoch {
			err1 := os.Remove(filepath.Join(wal.path, c.logName))
			err = multierr.Append(err, fmt.Errorf("removing WAL file %q: %w", c.logName, err1))

			continue
		}
		keptLogFiles = append(keptLogFiles, c)
	}
	wal.logs = keptLogFiles
	return nil
}

func (wal *WriteAheadLog[T, PT]) maybeRotate() error {
	if wal.active.file == nil {
		return wal.rotate()
	}
	stats, err := wal.active.file.Stat()
	if err != nil {
		return fmt.Errorf("collecting stats for the file: %w", err)
	}
	if stats.Size() > rotateAt {
		return wal.rotate()
	}
	return nil
}

// Close closes the existing file
// the WAL is safe to discard or can be used still
func (wal *WriteAheadLog[T, PT]) Close() error {
	wal.lk.Lock()
	defer wal.lk.Unlock()
	return wal.flush()
}

// Rotate closes the existing file
func (wal *WriteAheadLog[T, PT]) Rotate() error {
	wal.lk.Lock()
	defer wal.lk.Unlock()
	return wal.flush()
}

// rotate a log file
// it finalizes an active file if it is open
// and opens a new one
func (wal *WriteAheadLog[T, PT]) rotate() error {
	if wal.active.file != nil {
		if err := wal.flush(); err != nil {
			return fmt.Errorf("finalizing log file %q: %w", wal.active.logName, err)
		}
	}

	wal.active.logName = time.Now().UTC().Format(time.RFC3339Nano) + walExtension
	var err error
	wal.active.file, err = os.OpenFile(
		filepath.Join(wal.path, wal.active.logName),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)

	if err != nil {
		wal.active.file = nil
		wal.active.entriesLog = entriesLog[T]{}
		return fmt.Errorf("opening new log file %q: %w", wal.active.logName, err)
	}
	return nil
}

// flush the existing log
func (wal *WriteAheadLog[T, PT]) flush() error {
	if wal.active.file == nil {
		return nil
	}
	err := wal.active.file.Sync()
	if err != nil {
		return fmt.Errorf("syncing content to disk: %w", err)
	}
	err = wal.active.file.Close()
	if err != nil {
		return fmt.Errorf("closing the file: %w", err)
	}
	wal.logs = append(wal.logs, wal.active.entriesLog)

	wal.active.file = nil
	wal.active.entriesLog = entriesLog[T]{}

	return nil
}

func (wal *WriteAheadLog[T, PT]) hydrate() error {
	dirEntries, err := os.ReadDir(wal.path)

	if errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(wal.path, 0777)
		if err != nil {
			return fmt.Errorf("making WAL directory at %q: %w", wal.path, err)
		}
	} else if err != nil {
		return fmt.Errorf("reading dir entry at %q: %w", wal.path, err)
	}

	slices.SortFunc(dirEntries, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	for _, dir := range dirEntries {
		if !strings.HasSuffix(dir.Name(), walExtension) {
			continue
		}
		stat := entriesLog[T]{logName: dir.Name()}
		switch err := wal.forEachEntry(stat.logName, func(entry T) bool {
			stat.maxEpoch = max(stat.maxEpoch, PT(&entry).WALEpoch())
			stat.entries = append(stat.entries, entry)
			return true
		}); {
		case errors.Is(err, ErrParsingEntry):
			log.Warnw("Ignored log entry parse error while hydrating", "log", stat.logName, "err", err)
		case err != nil:
			return fmt.Errorf("reading log file: %w", err)
		}
		wal.logs = append(wal.logs, stat)
	}
	return nil
}

func (wal *WriteAheadLog[T, PT]) forEachEntry(logName string, visitor Visitor[T]) (_err error) {
	if len(logName) == 0 {
		return errors.New("log name cannot be empty")
	}
	target, err := os.Open(filepath.Clean(filepath.Join(wal.path, logName)))
	if err != nil {
		return fmt.Errorf("opening logfile %q: %w", logName, err)
	}
	defer func() {
		if err := target.Close(); err != nil {
			log.Errorw("failed to close log while reading", "name", logName, "err", err)
			_err = multierr.Append(_err, err)
		}
	}()
	reader := cbg.NewCborReader(target)
	for {
		var entry T
		switch err := PT(&entry).UnmarshalCBOR(reader); {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			log.Warnw("Failed to parse entry in log", "log", logName, "err", err)
			return fmt.Errorf("%w: %w", ErrParsingEntry, err)
		default:
			if proceed := visitor(entry); !proceed {
				return nil
			}
		}
	}
}
