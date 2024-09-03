package gpbft

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// TipSetKey is the canonically ordered concatenation of the block CIDs in a tipset.
type TipSetKey = []byte

const (
	// CidMaxLen specifies the maximum length of a CID.
	CidMaxLen = 38
	// ChainMaxLen specifies the maximum length of a chain value.
	ChainMaxLen = 100
	// TipsetKeyMaxLen specifies the maximum length of a tipset. The max size is
	// chosen such that it allows ample space for an impossibly-unlikely number of
	// blocks in a tipset, while maintaining a practical limit to prevent abuse.
	TipsetKeyMaxLen = 20 * CidMaxLen
)

// This the CID "prefix" of a v1-DagCBOR-Blake2b256-32 CID. That is:
var CidPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.DagCBOR,
	MhType:   multihash.BLAKE2B_MIN + 31,
	MhLength: 32,
}

// Hashes the given data and returns a CBOR + blake2b-256 CID.
func MakeCid(data []byte) cid.Cid {
	k, err := CidPrefix.Sum(data)
	if err != nil {
		panic(err)
	}
	return k
}

// TipSet represents a single EC tipset.
type TipSet struct {
	// The EC epoch (strictly increasing).
	Epoch int64
	// The tipset's key (canonically ordered concatenated block-header CIDs).
	Key TipSetKey `cborgen:"maxlen=760"` // 20 * 38B
	// Blake2b256-32 CID of the CBOR-encoded power table.
	PowerTable cid.Cid
	// Keccak256 root hash of the commitments merkle tree.
	Commitments [32]byte
}

// Validates a tipset.
// Note the zero value is invalid.
func (ts *TipSet) Validate() error {
	if len(ts.Key) == 0 {
		return errors.New("tipset key must not be empty")
	}
	if len(ts.Key) > TipsetKeyMaxLen {
		return errors.New("tipset key too long")
	}
	if !ts.PowerTable.Defined() {
		return errors.New("power table CID must not be empty")
	}
	if ts.PowerTable.ByteLen() > CidMaxLen {
		return errors.New("power table CID too long")
	}
	return nil
}

func (ts *TipSet) IsZero() bool {
	return len(ts.Key) == 0
}

func (ts *TipSet) Equal(b *TipSet) bool {
	return ts.Epoch == b.Epoch &&
		bytes.Equal(ts.Key, b.Key) &&
		ts.PowerTable == b.PowerTable &&
		ts.Commitments == b.Commitments
}

func (ts *TipSet) MarshalForSigning() []byte {
	var buf bytes.Buffer
	buf.Grow(len(ts.Key) + 4) // slight over-estimation
	_ = cbg.WriteByteArray(&buf, ts.Key)
	tsCid := MakeCid(buf.Bytes())
	buf.Reset()
	buf.Grow(tsCid.ByteLen() + ts.PowerTable.ByteLen() + 32 + 8)
	// epoch || commitments || tipset || powertable
	_ = binary.Write(&buf, binary.BigEndian, ts.Epoch)
	_, _ = buf.Write(ts.Commitments[:])
	_, _ = buf.Write(tsCid.Bytes())
	_, _ = buf.Write(ts.PowerTable.Bytes())
	return buf.Bytes()
}

func (ts *TipSet) String() string {
	if ts == nil {
		return "<nil>"
	}
	encTs := base32.StdEncoding.EncodeToString(ts.Key)

	return fmt.Sprintf("%s@%d", encTs[:min(16, len(encTs))], ts.Epoch)
}

// A chain of tipsets comprising a base (the last finalised tipset from which the chain extends).
// and (possibly empty) suffix.
// Tipsets are assumed to be built contiguously on each other,
// though epochs may be missing due to null rounds.
// The zero value is not a valid chain, and represents a "bottom" value
// when used in a Granite message.
type ECChain []TipSet

// A map key for a chain. The zero value means "bottom".
type ChainKey string

// Creates a new chain.
func NewChain(base TipSet, suffix ...TipSet) (ECChain, error) {
	var chain ECChain = []TipSet{base}
	chain = append(chain, suffix...)
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	return chain, nil
}

func (c ECChain) IsZero() bool {
	return len(c) == 0
}

func (c ECChain) HasSuffix() bool {
	return len(c.Suffix()) != 0
}

// Returns the base tipset.
func (c ECChain) Base() *TipSet {
	return &c[0]
}

// Returns the suffix of the chain after the base.
// An empty slice for a zero value.
func (c ECChain) Suffix() []TipSet {
	if c.IsZero() {
		return nil
	}
	return c[1:]
}

// Returns the last tipset in the chain.
// This could be the base tipset if there is no suffix.
// This will panic on a zero value.
func (c ECChain) Head() *TipSet {
	return &c[len(c)-1]
}

// Returns a new chain with the same base and no suffix.
// Invalid for a zero value.
func (c ECChain) BaseChain() ECChain {
	return ECChain{c[0]}
}

func (c ECChain) Extend(tips ...TipSetKey) ECChain {
	c = c[:len(c):len(c)]
	offset := c.Head().Epoch + 1
	pt := c.Head().PowerTable
	for i, tip := range tips {
		c = append(c, TipSet{
			Epoch:      offset + int64(i),
			Key:        tip,
			PowerTable: pt,
		})
	}
	return c
}

// Returns a chain with suffix (after the base) truncated to a maximum length.
// Prefix(0) returns the base chain.
// Invalid for a zero value.
func (c ECChain) Prefix(to int) ECChain {
	if c.IsZero() {
		panic("can't get prefix from zero-valued chain")
	}
	length := min(to+1, len(c))
	return c[:length:length]
}

// Compares two ECChains for equality.
func (c ECChain) Eq(other ECChain) bool {
	if len(c) != len(other) {
		return false
	}
	for i := range c {
		c[i].Equal(&other[i])
	}
	return true
}

// Checks whether two chains have the same base.
// Always false for a zero value.
func (c ECChain) SameBase(other ECChain) bool {
	if c.IsZero() || other.IsZero() {
		return false
	}
	return c.Base().Equal(other.Base())
}

// Check whether a chain has a specific base tipset.
// Always false for a zero value.
func (c ECChain) HasBase(t *TipSet) bool {
	return !t.IsZero() && !c.IsZero() && c.Base().Equal(t)
}

// Checks whether a chain has some prefix (including the base).
// Always false for a zero value.
func (c ECChain) HasPrefix(other ECChain) bool {
	if c.IsZero() || other.IsZero() {
		return false
	}
	if len(other) > len(c) {
		return false
	}
	for i := range other {
		if !c[i].Equal(&other[i]) {
			return false
		}
	}
	return true
}

// Checks whether a chain has some tipset (including as its base).
func (c ECChain) HasTipset(t *TipSet) bool {
	if t.IsZero() {
		// Chain can never contain zero-valued TipSet.
		return false
	}
	for i := range c {
		if c[i].Equal(t) {
			return true
		}
	}
	return false
}

// Validates a chain value, returning an error if it finds any issues.
// A chain is valid if it meets the following criteria:
// 1) All contained tipsets are non-empty.
// 2) All epochs are >= 0 and increasing.
// 3) The chain is not longer than ChainMaxLen.
// An entirely zero-valued chain itself is deemed valid. See ECChain.IsZero.
func (c ECChain) Validate() error {
	if c.IsZero() {
		return nil
	}
	if len(c) > ChainMaxLen {
		return errors.New("chain too long")
	}
	var lastEpoch int64 = -1
	for i := range c {
		ts := &c[i]
		if err := ts.Validate(); err != nil {
			return fmt.Errorf("tipset %d: %w", i, err)
		}
		if ts.Epoch <= lastEpoch {
			return fmt.Errorf("chain must have increasing epochs %d <= %d", ts.Epoch, lastEpoch)
		}
		lastEpoch = ts.Epoch
	}
	return nil
}

// Returns an identifier for the chain suitable for use as a map key.
// This must completely determine the sequence of tipsets in the chain.
func (c ECChain) Key() ChainKey {
	ln := len(c) * (8 + 32 + 4) // epoch + commitement + ts length
	for i := range c {
		ln += len(c[i].Key) + c[i].PowerTable.ByteLen()
	}
	var buf bytes.Buffer
	buf.Grow(ln)
	for i := range c {
		ts := &c[i]
		_ = binary.Write(&buf, binary.BigEndian, ts.Epoch)
		_, _ = buf.Write(ts.Commitments[:])
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(ts.Key)))
		buf.Write(ts.Key)
		_, _ = buf.Write(ts.PowerTable.Bytes())
	}
	return ChainKey(buf.String())
}

func (c ECChain) String() string {
	if len(c) == 0 {
		return "丄"
	}
	var b strings.Builder
	b.WriteString("[")
	for i := range c {
		b.WriteString(c[i].String())
		if i < len(c)-1 {
			b.WriteString(", ")
		}
		if b.Len() > 77 {
			b.WriteString("...")
			break
		}
	}
	b.WriteString("]")
	b.WriteString(fmt.Sprintf("len(%d)", len(c)))
	return b.String()
}
