package certstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-state-types/cbor"
)

var ErrUnknownLatestCertificate = errors.New("latest certificate is not known")

// ExportLatestSnapshot exports an F3 snapshot that includes the finality certificate chain until the current `latestCertificate`.
//
// Checkout the format specification at <https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0108.md>
func (cs *Store) ExportLatestSnapshot(ctx context.Context, writer io.Writer) error {
	if cs.latestCertificate == nil {
		return ErrUnknownLatestCertificate
	}
	return cs.ExportSnapshot(ctx, cs.latestCertificate.GPBFTInstance, writer)
}

// ExportSnapshot exports an F3 snapshot that includes the finality certificate chain from the `Store.firstInstance` to the specified `lastInstance`.
//
// Checkout the format specification at <https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0108.md>
func (cs *Store) ExportSnapshot(ctx context.Context, latestInstance uint64, writer io.Writer) error {
	initialPowerTable, err := cs.GetPowerTable(ctx, cs.firstInstance)
	if err != nil {
		return fmt.Errorf("failed to get initial power table at instance %d: %w", cs.firstInstance, err)
	}
	header := SnapshotHeader{1, cs.firstInstance, latestInstance, initialPowerTable}
	if _, err := header.WriteTo(writer); err != nil {
		return fmt.Errorf("failed to write snapshot header: %w", err)
	}
	for i := cs.firstInstance; i <= latestInstance; i++ {
		cert, err := cs.ds.Get(ctx, cs.keyForCert(i))
		if err != nil {
			return fmt.Errorf("failed to get certificate at instance %d:: %w", i, err)
		}
		buffer := bytes.NewBuffer(cert)
		if _, err := writeSnapshotBlockBytes(writer, buffer); err != nil {
			return err
		}
	}
	return nil
}

type SnapshotHeader struct {
	Version           uint64
	FirstInstance     uint64
	LatestInstance    uint64
	InitialPowerTable gpbft.PowerEntries
}

func (h *SnapshotHeader) WriteTo(w io.Writer) (int64, error) {
	return writeSnapshotCborEncodedBlock(w, h)
}

// writeSnapshotCborEncodedBlock writes CBOR-encoded header or data block with a varint-encoded length prefix
func writeSnapshotCborEncodedBlock(writer io.Writer, block cbor.Marshaler) (int64, error) {
	var buffer bytes.Buffer
	if err := block.MarshalCBOR(&buffer); err != nil {
		return 0, err
	}
	return writeSnapshotBlockBytes(writer, &buffer)
}

// writeSnapshotBlockBytes writes header or data block with a varint-encoded length prefix
func writeSnapshotBlockBytes(writer io.Writer, buffer *bytes.Buffer) (int64, error) {
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, uint64(buffer.Len()))
	len1, err := bytes.NewBuffer(buf[:n]).WriteTo(writer)
	if err != nil {
		return 0, err
	}
	len2, err := buffer.WriteTo(writer)
	if err != nil {
		return 0, err
	}
	return len1 + len2, nil
}
