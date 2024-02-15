package f3

import (
	"io"
	"math/big"
	"strings"

	"github.com/ipfs/go-datastore"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

type ActorID uint64

type StoragePower = big.Int

type PubKey []byte

// NetworkName provides separation between different networks
// it is implicitly included in all signatures and VRFs
type NetworkName string

func (nn NetworkName) DatastorePrefix() datastore.Key {
	return datastore.NewKey("/f3/" + string(nn))
}
func (nn NetworkName) PubSubTopic() string {
	return "/f3/granite/0.0.1/" + string(nn)
}

// Creates a new StoragePower struct with a specific value and returns the result
func NewStoragePower(value int64) *StoragePower {
	return new(big.Int).SetInt64(value)
}

// Opaque type identifying a tipset.
// This is expected to be a concatenation of CIDs of block headers identifying a tipset.
// GossipPBFT doesn't need to know anything about CID format or behaviour, so adapts to this simple type
// rather than import all the dependencies of the CID package.
type TipSetID struct {
	// The inner value is a string so that this type can be used as a map key (like the go-cid package).
	value string
}

var _ cbg.CBORMarshaler = TipSetID{}
var _ cbg.CBORUnmarshaler = (*TipSetID)(nil)

func (t TipSetID) MarshalCBOR(w io.Writer) error {
	return cbg.WriteByteArray(w, []byte(t.value))
}

func (t *TipSetID) UnmarshalCBOR(r io.Reader) error {
	b, err := cbg.ReadByteArray(r, cbg.ByteArrayMaxLen)
	if err != nil {
		return xerrors.Errorf("decoding TipSetID from CBOR: %w", err)
	}
	t.value = string(b)
	return nil
}

// Creates a new TipSetID from a byte array.
func NewTipSetID(b []byte) TipSetID {
	return TipSetID{string(b)}
}

// Creates a new TipSetID from a string.
func NewTipSetIDFromString(s string) TipSetID {
	return TipSetID{s}
}

// Returns a zero-value TipSetID.
func ZeroTipSetID() TipSetID {
	return TipSetID{}
}

// Checks whether a tipset ID is zero.
func (t TipSetID) IsZero() bool {
	return t.value == ""
}

// Orders two tipset IDs.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// This is a lexicographic ordering of the bytes of the IDs.
func (t TipSetID) Compare(o TipSetID) int {
	return strings.Compare(t.value, o.value)
}

func (t TipSetID) Bytes() []byte {
	return []byte(t.value)
}
