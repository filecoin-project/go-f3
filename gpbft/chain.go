package gpbft

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/crypto/blake2b"
)

// TipSetKey is the canonically ordered concatenation of the block CIDs in a tipset.
type TipSetKey = []byte

type CID = []byte

const CID_MAX_LEN = 38

// Allow ample space for an impossibly-unlikely number of blocks in a tipset,
// while maintaining a practical limit to prevent abuse.
const TIPSET_KEY_MAX_LEN = 20 * CID_MAX_LEN

// This the CID "prefix" of a v1-DagCBOR-Blake2b256-32 CID. That is:
//
// - 0x01     CIDv1
// - 0x71     DagCBOR
// - 0xA0E402 LEB128 encoded Blake2b256 multicodec
// - 0x20     32 (length of the digest)
var cidPrefix = []byte{0x01, 0x71, 0xA0, 0xE4, 0x02, 0x20}

// Hashes the given data and returns a CBOR + blake2b-256 CID.
func MakeCid(data []byte) []byte {
	// We construct this CID manually to avoid depending on go-cid (it's also a _bit_ faster).
	digest := blake2b.Sum256(data)

	return DigestToCid(digest[:])
}

// DigestToCid turns a digest into CBOR + blake2b-256 CID
func DigestToCid(digest []byte) []byte {
	if len(digest) != 32 {
		panic(fmt.Sprintf("wrong length of digest, expected 32, got %d", len(digest)))
	}
	out := make([]byte, 0, CID_MAX_LEN)
	out = append(out, cidPrefix...)
	out = append(out, digest...)
	return out
}

// TipSet represents a single EC tipset.
type TipSet struct {
	// The EC epoch (strictly increasing).
	Epoch int64
	// The tipset's key (canonically ordered concatenated block-header CIDs).
	Key TipSetKey `cborgen:"maxlen=760"` // 20 * 38B
	// Blake2b256-32 CID of the CBOR-encoded power table.
	PowerTable CID `cborgen:"maxlen=38"` // []PowerEntry
	// Keccak256 root hash of the commitments merkle tree.
	Commitments [32]byte
}

// Validates a tipset.
// Note the zero value is invalid.
func (ts *TipSet) Validate() error {
	if len(ts.Key) == 0 {
		return errors.New("tipset key must not be empty")
	}
	if len(ts.Key) > TIPSET_KEY_MAX_LEN {
		return errors.New("tipset key too long")
	}
	if len(ts.PowerTable) == 0 {
		return errors.New("power table CID must not be empty")
	}
	if len(ts.PowerTable) > CID_MAX_LEN {
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
		bytes.Equal(ts.PowerTable, b.PowerTable) &&
		ts.Commitments == b.Commitments
}

func (ts *TipSet) MarshalForSigning() []byte {
	var buf bytes.Buffer
	buf.Grow(len(ts.Key) + 4) // slight over-estimation
	_ = cbg.WriteByteArray(&buf, ts.Key)
	tsCid := MakeCid(buf.Bytes())
	buf.Reset()
	buf.Grow(len(tsCid) + len(ts.PowerTable) + 32 + 8)
	// epoch || commitments || tipset || powertable
	_ = binary.Write(&buf, binary.BigEndian, ts.Epoch)
	_, _ = buf.Write(ts.Commitments[:])
	_, _ = buf.Write(tsCid)
	_, _ = buf.Write(ts.PowerTable)
	return buf.Bytes()
}

func (ts *TipSet) String() string {
	if ts == nil {
		return "<nil>"
	}
	encTs := base32.StdEncoding.EncodeToString(ts.Key)

	return fmt.Sprintf("%s@%d", encTs[:16], ts.Epoch)
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

// Maximum length of a chain value.
const CHAIN_MAX_LEN = 100

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
// 3) The chain is not longer than CHAIN_MAX_LEN.
// An entirely zero-valued chain itself is deemed valid. See ECChain.IsZero.
func (c ECChain) Validate() error {
	if c.IsZero() {
		return nil
	}
	if len(c) > CHAIN_MAX_LEN {
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
		ln += len(c[i].Key) + len(c[i].PowerTable)
	}
	var buf bytes.Buffer
	buf.Grow(ln)
	for i := range c {
		ts := &c[i]
		_ = binary.Write(&buf, binary.BigEndian, ts.Epoch)
		_, _ = buf.Write(ts.Commitments[:])
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(ts.Key)))
		buf.Write(ts.Key)
		_, _ = buf.Write(ts.PowerTable)
	}
	return ChainKey(buf.String())
}

func (c ECChain) String() string {
	var b strings.Builder
	b.WriteString("[")
	for i := range c {
		b.WriteString(c[i].String())
		if i < len(c)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	str := b.String()
	if len(str) > 77 {
		str = str[:77] + "..."
	}
	return str
}
