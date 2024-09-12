package writeaheadlog

import (
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

	logFiles []logContent[T, PT]
	active   struct {
		file       *os.File
		cborWriter *cbg.CborWriter
		logContent[T, PT]
	}
}

type logContent[T any, PT interface {
	*T
	Entry
}] struct {
	logname  string
	content  []T
	maxEpoch uint64
	complete bool
}

func Open[T any, PT interface {
	*T
	Entry
}](directory string) (*WriteAheadLog[T, PT], error) {
	wal := WriteAheadLog[T, PT]{
		path: directory,
	}

	err := wal.readInAll()
	if err != nil {
		return nil, fmt.Errorf("reading the WAL: %v", err)
	}

	return &wal, nil
}

func (wal *WriteAheadLog[T, PT]) All() []T {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	var res []T
	for _, c := range wal.logFiles {
		res = append(res, c.content...)
	}
	res = append(res, wal.active.content...)
	return res
}

/*
// TODO: version file
// or mabye not todo, IDK
const versionFileName = "version"
const version = 0
func (wal *WriteAheadLog[T]) readVersion() (int, error) {
	versionContent, err := os.ReadFile(path.Join(wal.path, versionFileName))
	if errors.Is(err, os.ErrNotExist) {
		return -1, nil
	}
	if err != nil {
		return -1, fmt.Errorf("reading the version file: %w", err)
	}

	version, err := strconv.ParseInt(string(versionContent), 10, 32)
	if err != nil {
		return -1, fmt.Errorf("parsing the version number: %w", err)
	}

	return version, nil
}
*/

func (wal *WriteAheadLog[T, PT]) Log(value T) error {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	if err := wal.maybeRotate(); err != nil {
		return fmt.Errorf("attemting to rotate: %w", err)
	}

	err := PT(&value).MarshalCBOR(wal.active.cborWriter)
	if err != nil {
		return fmt.Errorf("saving value to WAL: %w", err)
	}
	err = wal.active.file.Sync()
	if err != nil {
		return fmt.Errorf("sycning the file: %w", err)
	}

	wal.active.maxEpoch = max(wal.active.maxEpoch, PT(&value).WALEpoch())
	wal.active.content = append(wal.active.content, value)

	return nil
}

// Purge removes files containing entries only order than keepEpoch
// It is not guarnateed to remove all entries older than keepEpoch as it will keep the current
// file.
func (wal *WriteAheadLog[T, PT]) Purge(keepEpoch uint64) error {
	wal.lk.Lock()
	defer wal.lk.Unlock()

	var keptLogFiles []logContent[T, PT]
	var err error
	for _, c := range wal.logFiles {
		if c.maxEpoch < keepEpoch {
			err1 := os.Remove(filepath.Join(wal.path, c.logname))
			err = multierr.Append(err, fmt.Errorf("removing WAL file %q: %w", c.logname, err1))

			continue
		}
		keptLogFiles = append(keptLogFiles, c)
	}
	wal.logFiles = keptLogFiles
	return nil
}

const roatateAt = 1 << 20 // 1MiB

func (wal *WriteAheadLog[T, PT]) maybeRotate() error {
	if wal.active.file == nil {
		return wal.rotate()
	}
	stats, err := wal.active.file.Stat()
	if err != nil {
		return fmt.Errorf("collecting stats for the file: %w", err)
	}
	if stats.Size() > roatateAt {
		return wal.rotate()
	}
	return nil
}

const walExtension = ".wal.cbor"

// Finalize closes the existing file
// the WAL is safe to discard or can be used still
func (wal *WriteAheadLog[T, PT]) Finalize() error {
	wal.lk.Lock()
	defer wal.lk.Unlock()
	return wal.finalize()
}

// rotate a log file
// it finalizes an active file if it is open
// and opens a new one
func (wal *WriteAheadLog[T, PT]) rotate() error {
	if wal.active.file != nil {
		if err := wal.finalize(); err != nil {
			return fmt.Errorf("finalizing log file: %w", err)
		}
	}

	wal.active.logname = time.Now().UTC().Format(time.RFC3339Nano) + walExtension
	var err error
	wal.active.file, err = os.OpenFile(
		filepath.Join(wal.path, wal.active.logname),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)

	if err != nil {
		wal.active.file = nil
		return fmt.Errorf("opening new log file %q: %w", wal.active.logname, err)
	}
	wal.active.cborWriter = cbg.NewCborWriter(wal.active.file)
	return nil
}

// finalize the existing log
func (wal *WriteAheadLog[T, PT]) finalize() error {
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
	wal.active.file = nil
	wal.active.cborWriter = nil

	wal.active.logContent.complete = true
	wal.logFiles = append(wal.logFiles, wal.active.logContent)
	wal.active.logContent = logContent[T, PT]{}
	return nil
}

func (wal *WriteAheadLog[T, PT]) readInAll() error {
	_, err := os.Stat(wal.path)
	if errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(wal.path, 0777)
		if err != nil {
			return fmt.Errorf("making WAL directory at %q: %w", wal.path, err)
		}
	} else if err != nil {
		return fmt.Errorf("checking WAL dir: %w", err)
	}

	dirEntries, err := os.ReadDir(wal.path)
	if err != nil {
		return fmt.Errorf("reading dir entries at %q: %w", wal.path, err)
	}
	slices.SortFunc(dirEntries, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), walExtension) {
			continue
		}
		content, err := wal.readLogFile(entry.Name())
		if err != nil {
			return fmt.Errorf("readling log file: %w", err)
		}

		wal.logFiles = append(wal.logFiles, content)
	}
	return nil
}

// readLogFile reads in a file logname as a log
// it eats parsing errors and logs in on purpose
func (wal *WriteAheadLog[T, PT]) readLogFile(logname string) (logContent[T, PT], error) {
	var res logContent[T, PT]
	logFile, err := os.Open(filepath.Join(wal.path, logname))
	if err != nil {
		return res, fmt.Errorf("opening logfile %q: %w", logname, err)
	}
	defer logFile.Close()

	logFileReader := cbg.NewCborReader(logFile)
	var content []T
	var maxEpoch uint64
	for err == nil {
		var single T
		err = PT(&single).UnmarshalCBOR(logFileReader)
		if err == nil {
			content = append(content, single)
			maxEpoch = max(maxEpoch, PT(&single).WALEpoch())
		}
	}

	res = logContent[T, PT]{
		logname:  logname,
		content:  content,
		maxEpoch: maxEpoch,
		complete: true,
	}

	switch {
	case err == nil:
		return res, nil
	case errors.Is(err, io.EOF):
		return res, nil
	default:
		log.Errorf("got error while reading WAL log file %q: %+v", logname, err)
		res.complete = false
		return res, nil
	}
}
