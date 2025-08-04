package certstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-state-types/cbor"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	"github.com/multiformats/go-multihash"
	"golang.org/x/crypto/blake2b"
)

var ErrUnknownLatestCertificate = errors.New("latest certificate is not known")

// ExportLatestSnapshot exports an F3 snapshot that includes the finality certificate chain until the current `latestCertificate`.
//
// Checkout the snapshot format specification at <https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0108.md>
func (cs *Store) ExportLatestSnapshot(ctx context.Context, writer io.Writer) (cid.Cid, *SnapshotHeader, error) {
	if cs.latestCertificate == nil {
		return cid.Undef, nil, ErrUnknownLatestCertificate
	}
	return cs.ExportSnapshot(ctx, cs.latestCertificate.GPBFTInstance, writer)
}

// ExportSnapshot exports an F3 snapshot that includes the finality certificate chain from the `Store.firstInstance` to the specified `lastInstance`.
//
// Checkout the snapshot format specification at <https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0108.md>
func (cs *Store) ExportSnapshot(ctx context.Context, latestInstance uint64, writer io.Writer) (cid.Cid, *SnapshotHeader, error) {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		return cid.Undef, nil, err
	}
	hashWriter := hashWriter{hasher, writer}
	initialPowerTable, err := cs.GetPowerTable(ctx, cs.firstInstance)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to get initial power table at instance %d: %w", cs.firstInstance, err)
	}
	header := SnapshotHeader{1, cs.firstInstance, latestInstance, initialPowerTable}
	if _, err := header.WriteTo(hashWriter); err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to write snapshot header: %w", err)
	}
	for i := cs.firstInstance; i <= latestInstance; i++ {
		cert, err := cs.ds.Get(ctx, cs.keyForCert(i))
		if err != nil {
			return cid.Undef, nil, fmt.Errorf("failed to get certificate at instance %d:: %w", i, err)
		}
		buffer := bytes.NewBuffer(cert)
		if _, err := writeSnapshotBlockBytes(hashWriter, buffer); err != nil {
			return cid.Undef, nil, err
		}
	}
	hash := hashWriter.hasher.Sum(nil)
	mh, err := multihash.Encode(hash, multihash.BLAKE2B_MIN+31)
	if err != nil {
		return cid.Undef, nil, err
	}

	return cid.NewCidV1(cid.Raw, mh), &header, nil
}

type hashWriter struct {
	hasher hash.Hash
	writer io.Writer
}

func (w hashWriter) Write(p []byte) (n int, err error) {
	if _, err := w.hasher.Write(p); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

type SnapshotReader interface {
	io.Reader
	io.ByteReader
}

// ImportSnapshotToDatastore imports an F3 snapshot into the specified Datastore
//
// Checkout the snapshot format specification at <https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0108.md>
func ImportSnapshotToDatastore(ctx context.Context, snapshot SnapshotReader, ds datastore.Batching) error {
	return importSnapshotToDatastoreWithTestingPowerTableFrequency(ctx, snapshot, ds, 0)
}

func importSnapshotToDatastoreWithTestingPowerTableFrequency(ctx context.Context, snapshot SnapshotReader, ds datastore.Batching, testingPowerTableFrequency uint64) error {
	headerBytes, err := readSnapshotBlockBytes(snapshot)
	if err != nil {
		return err
	}
	var header SnapshotHeader
	err = header.UnmarshalCBOR(bytes.NewReader(headerBytes))
	if err != nil {
		return fmt.Errorf("failed to decode snapshot header: %w", err)
	}
	dsb := autobatch.NewAutoBatching(ds, 1000)
	defer dsb.Flush(ctx)
	cs, err := OpenOrCreateStore(ctx, dsb, header.FirstInstance, header.InitialPowerTable)
	if err != nil {
		return err
	}
	if testingPowerTableFrequency > 0 {
		cs.powerTableFrequency = testingPowerTableFrequency
	}
	var latestCert *certs.FinalityCertificate
	ptm := certs.PowerTableArrayToMap(header.InitialPowerTable)
	for i := header.FirstInstance; ; i += 1 {
		certBytes, err := readSnapshotBlockBytes(snapshot)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to decode finality certificate: %w", err)
		}

		var cert certs.FinalityCertificate
		if err = cert.UnmarshalCBOR(bytes.NewReader(certBytes)); err != nil {
			return err
		}
		latestCert = &cert

		if i != cert.GPBFTInstance {
			return fmt.Errorf("the certificate of instance %d is missing", i)
		}

		if i > header.LatestInstance {
			return fmt.Errorf("certificate of instance %d is found, expected latest instance %d", i, header.LatestInstance)
		}

		if err := cs.ds.Put(ctx, cs.keyForCert(cert.GPBFTInstance), certBytes); err != nil {
			return err
		}

		if ptm, err = certs.ApplyPowerTableDiffsToMap(ptm, cert.PowerTableDelta); err != nil {
			return err
		}

		if (cert.GPBFTInstance+1)%cs.powerTableFrequency == 0 {
			pt := certs.PowerTableMapToArray(ptm)
			if err = checkPowerTable(pt, cert.SupplementalData.PowerTable); err != nil {
				return err
			}
			if err := cs.putPowerTable(ctx, cert.GPBFTInstance+1, pt); err != nil {
				return err
			}
		}
	}

	pt := certs.PowerTableMapToArray(ptm)
	if err = checkPowerTable(pt, latestCert.SupplementalData.PowerTable); err != nil {
		return err
	}

	return cs.writeInstanceNumber(ctx, certStoreLatestKey, header.LatestInstance)
}

func checkPowerTable(pt gpbft.PowerEntries, expectedCid cid.Cid) error {
	ptCid, err := certs.MakePowerTableCID(pt)
	if err != nil {
		return err
	}
	if ptCid != expectedCid {
		return fmt.Errorf("new power table differs from expected power table: %s != %s", ptCid, expectedCid)
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

func readSnapshotBlockBytes(reader SnapshotReader) ([]byte, error) {
	n1, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, n1)
	n2, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	if n2 != int(n1) {
		return nil, fmt.Errorf("incomplete block, %d bytes expected, %d bytes got", n1, n2)
	}
	return buf, nil
}
