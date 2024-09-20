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

var log = logging.Logger("f3/wal")

const (
	rotateAt     = 1 << 20 // 1MiB
	walExtension = ".wal.cbor"
)

type Entry interface {
	WALEpoch() uint64
	cbg.CBORMarshaler
	cbg.CBORUnmarshaler
}

type WriteAheadLog[T any, PT interface {
	*T
	Entry
}] struct {
	// pointer type trickery above
	lk sync.Mutex

	path string

	logFiles []logStat
	active   struct {
		file *os.File
		logStat
	}
}

type logStat struct {
	logName  string
	maxEpoch uint64
}

func Open[T any, PT interface {
	*T
	Entry
}](directory string) (*WriteAheadLog[T, PT], error) {
	wal := WriteAheadLog[T, PT]{
		path: directory,
	}

	err := wal.hydrate()
	if err != nil {
		return nil, fmt.Errorf("reading the WAL: %w", err)
	}

	return &wal, nil
}

func (wal *WriteAheadLog[T, PT]) All() ([]T, error) {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	var res []T

	for _, s := range wal.logFiles {
		_, content, err := wal.readLogFile(s.logName, true)
		if err != nil {
			return nil, fmt.Errorf("reading log file %q to list: %w", s.logName, err)
		}
		res = append(res, content...)
	}
	if wal.active.file != nil {
		_, content, err := wal.readLogFile(wal.active.logName, true)
		if err != nil {
			return nil, fmt.Errorf("reading active log file %q to list: %w", wal.active.logName, err)
		}
		res = append(res, content...)
	}

	return res, nil
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

	return nil
}

// Purge removes files containing entries only older than keepEpoch
// It is not guaranteed to remove all entries older than keepEpoch as it will keep the current
// file.
func (wal *WriteAheadLog[T, PT]) Purge(keepEpoch uint64) error {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	var keptLogFiles []logStat
	var err error
	for _, c := range wal.logFiles {
		if c.maxEpoch < keepEpoch {
			err1 := os.Remove(filepath.Join(wal.path, c.logName))
			err = multierr.Append(err, fmt.Errorf("removing WAL file %q: %w", c.logName, err1))

			continue
		}
		keptLogFiles = append(keptLogFiles, c)
	}
	wal.logFiles = keptLogFiles
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

// Flush closes the existing file
// the WAL is safe to discard or can be used still
func (wal *WriteAheadLog[T, PT]) Flush() error {
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
			return fmt.Errorf("finalizing log file: %w", err)
		}
	}

	wal.active.logName = time.Now().UTC().Format(time.RFC3339Nano) + walExtension
	var err error
	wal.active.file, err = os.OpenFile(
		filepath.Join(wal.path, wal.active.logName),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)

	if err != nil {
		wal.active.file = nil
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
	wal.logFiles = append(wal.logFiles, wal.active.logStat)

	wal.active.file = nil
	wal.active.logStat = logStat{}

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

	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), walExtension) {
			continue
		}
		logS, _, err := wal.readLogFile(entry.Name(), false)
		if err != nil {
			return fmt.Errorf("readling log file: %w", err)
		}

		wal.logFiles = append(wal.logFiles, logS)
	}
	return nil
}

// readLogFile reads in a file logname as a log
// it eats parsing errors and logs in on purpose
func (wal *WriteAheadLog[T, PT]) readLogFile(logname string, keepVaues bool) (logStat, []T, error) {
	var res logStat
	if len(logname) == 0 {
		return res, nil, errors.New("empty logname")
	}

	logFile, err := os.Open(filepath.Join(wal.path, logname))
	if err != nil {
		return res, nil, fmt.Errorf("opening logfile %q: %w", logname, err)
	}
	defer logFile.Close()

	logFileReader := cbg.NewCborReader(logFile)
	var content []T
	var maxEpoch uint64
	for err == nil {
		var single T
		err = PT(&single).UnmarshalCBOR(logFileReader)
		if err == nil {
			if keepVaues {
				content = append(content, single)
			}
			maxEpoch = max(maxEpoch, PT(&single).WALEpoch())
		}
	}

	res = logStat{
		logName:  logname,
		maxEpoch: maxEpoch,
	}

	switch {
	case err == nil:
		// the EOF case is expected, this is slightly unexpected but still handle it
		return res, content, nil
	case errors.Is(err, io.EOF):
		return res, content, nil
	default:
		log.Errorf("got error while reading WAL log file %q: %+v", logname, err)
		return res, content, nil
	}
}
