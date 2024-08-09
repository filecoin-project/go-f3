package msgdump

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type RowType = ParquetEnvelope
type ParquetWriter struct {
	schema *parquet.Schema
	path   string

	f *os.File
	w *parquet.GenericWriter[RowType]
}

func NewParquetWriter(path string) (*ParquetWriter, error) {
	pw := ParquetWriter{
		path: path,
	}

	err := os.MkdirAll(path, 0770)
	if err != nil {
		return nil, fmt.Errorf("creating directory: %w", err)
	}

	pw.f, pw.w, err = pw.openNewFile()
	return &pw, err
}

var parquetExt = ".parquet"
var partialExt = ".partial"

func (pw *ParquetWriter) openNewFile() (*os.File, *parquet.GenericWriter[RowType], error) {
	filename := "gen0-" + time.Now().Format(time.RFC3339) + parquetExt + partialExt
	f, err := os.OpenFile(filepath.Join(pw.path, filename), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("creating a parquet file: %w", err)
	}
	w := parquet.NewGenericWriter[RowType](f, parquet.Compression(&zstd.Codec{Level: zstd.SpeedFastest}))
	return f, w, nil
}

func (pw *ParquetWriter) WriteRows(rows []RowType) (int, error) {
	return pw.w.Write(rows)
}
func (pw *ParquetWriter) Write(row RowType) (int, error) {
	return pw.w.Write([]RowType{row})
}

func (pw *ParquetWriter) Rotate() error {
	f, w, err := pw.openNewFile()
	if err != nil {
		return fmt.Errorf("while opening new file: %w", err)
	}

	err = pw.finalize()
	if err != nil {
		return fmt.Errorf("while flushing a file: %w", err)
	}
	pw.f = f
	pw.w = w
	return nil
}

func (pw *ParquetWriter) Close() error {
	return pw.finalize()
}

func (pw *ParquetWriter) finalize() error {
	err := pw.w.Close()
	if err != nil {
		return fmt.Errorf("closing ParquetWriter: %w", err)
	}
	fileName := pw.f.Name()
	fileNameFinal := strings.TrimSuffix(fileName, partialExt)
	err = pw.f.Close()
	if err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	err = os.Rename(fileName, fileNameFinal)
	if err != nil {
		return fmt.Errorf("renaming file: %w", err)
	}
	return nil
}
