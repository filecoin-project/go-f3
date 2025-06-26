package certstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-datastore"
	xerrors "golang.org/x/xerrors"
)

var ErrlatestCertificateNil = errors.New("latest certificate is not available")

// Exports an F3 snapshot that includes the finality certificate chain until the current `latestCertificate`.
func (cs *Store) ExportLatestSnapshot(ctx context.Context, writer io.Writer) error {
	if cs.latestCertificate == nil {
		return ErrlatestCertificateNil
	}
	return cs.ExportSnapshot(ctx, cs.latestCertificate.GPBFTInstance, writer)
}

// Exports an F3 snapshot that includes the finality certificate chain until the specified `lastInstance`.
func (cs *Store) ExportSnapshot(ctx context.Context, lastInstance uint64, writer io.Writer) error {
	initialPowerTable, err := cs.GetPowerTable(ctx, cs.firstInstance)
	if err != nil {
		return xerrors.Errorf("failed to get initial power table at instance %d: %w", cs.firstInstance, err)
	}
	header := SnapshotHeader{1, cs.firstInstance, lastInstance, initialPowerTable}
	if err := header.WriteToSnapshot(writer); err != nil {
		return xerrors.Errorf("failed to write snapshot header: %w", err)
	}
	for i := cs.firstInstance; i <= lastInstance; i++ {
		cert, err := cs.ds.Get(ctx, cs.keyForCert(i))
		if err != nil {
			return xerrors.Errorf("failed to get certificate at instance %d:: %w", i, err)
		}
		buffer := bytes.NewBuffer(cert)
		if err := writeSnapshotBlockBytes(writer, buffer); err != nil {
			return err
		}
	}
	return nil
}

// Imports an F3 snapshot and opens the certificate store.
//
// The passed Datastore has to be thread safe.
func ImportSnapshotAndOpenStore(ctx context.Context, ds datastore.Datastore) error {
	return xerrors.New("to be implemented")
}

type SnapshotHeader struct {
	Version           uint64
	FirstInstance     uint64
	LatestInstance    uint64
	InitialPowerTable gpbft.PowerEntries
}

func (h *SnapshotHeader) WriteToSnapshot(writer io.Writer) error {
	return writeSnapshotCborEncodedBlock(writer, h)
}

// Writes CBOR-encoded header or data block with a varint-encoded length prefix
func writeSnapshotCborEncodedBlock(writer io.Writer, block MarshalCBOR) error {
	var buffer bytes.Buffer
	if err := block.MarshalCBOR(&buffer); err != nil {
		return err
	}
	return writeSnapshotBlockBytes(writer, &buffer)
}

// Writes header or data block with a varint-encoded length prefix
func writeSnapshotBlockBytes(writer io.Writer, buffer *bytes.Buffer) error {
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, uint64(buffer.Len()))
	if _, err := writer.Write(buf[:n]); err != nil {
		return err
	}
	if _, err := buffer.WriteTo(writer); err != nil {
		return err
	}
	return nil
}

type MarshalCBOR interface {
	MarshalCBOR(w io.Writer) error
}
