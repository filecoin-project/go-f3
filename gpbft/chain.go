package gpbft

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/filecoin-project/go-f3/merkle"
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
	ChainMaxLen = 128
	// ChainDefaultLen specifies the default length of chain value.
	ChainDefaultLen = 100
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
	Commitments [32]byte `cborgen:"maxlen=32"`
}

// Validates a tipset.
// Note the zero value is invalid.
func (ts *TipSet) Validate() error {
	if ts == nil {
		return errors.New("tipset must not be nil")
	}
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

func (ts *TipSet) Equal(b *TipSet) bool {
	if ts == nil || b == nil {
		return ts == b
	}
	return ts.Epoch == b.Epoch &&
		bytes.Equal(ts.Key, b.Key) &&
		ts.PowerTable.Equals(b.PowerTable) &&
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

// Custom JSON marshalling for TipSet to achieve:
// 1. a standard TipSetKey representation that presents an array of dag-json CIDs.
// 2. a commitment field that is a base64-encoded string.

type tipSetSub TipSet
type tipSetJson struct {
	Key         []cid.Cid
	Commitments []byte
	*tipSetSub
}

func (ts TipSet) MarshalJSON() ([]byte, error) {
	cids, err := cidsFromTipSetKey(ts.Key)
	if err != nil {
		return nil, err
	}
	return json.Marshal(&tipSetJson{
		Key:         cids,
		Commitments: ts.Commitments[:],
		tipSetSub:   (*tipSetSub)(&ts),
	})
}

func (ts *TipSet) UnmarshalJSON(b []byte) error {
	aux := &tipSetJson{tipSetSub: (*tipSetSub)(ts)}
	var err error
	if err = json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if ts.Key, err = tipSetKeyFromCids(aux.Key); err != nil {
		return err
	}
	if len(aux.Commitments) != 32 {
		return errors.New("commitments must be 32 bytes")
	}
	copy(ts.Commitments[:], aux.Commitments)
	return nil
}

func cidsFromTipSetKey(encoded []byte) ([]cid.Cid, error) {
	var cids []cid.Cid
	for nextIdx := 0; nextIdx < len(encoded); {
		nr, c, err := cid.CidFromBytes(encoded[nextIdx:])
		if err != nil {
			return nil, err
		}
		cids = append(cids, c)
		nextIdx += nr
	}
	return cids, nil
}

func tipSetKeyFromCids(cids []cid.Cid) (TipSetKey, error) {
	var buf bytes.Buffer
	for _, c := range cids {
		if _, err := buf.Write(c.Bytes()); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

var (
	_ json.Marshaler    = (*ECChain)(nil)
	_ json.Unmarshaler  = (*ECChain)(nil)
	_ cbg.CBORMarshaler = (*ECChain)(nil)
	_ cbg.CBORMarshaler = (*ECChain)(nil)
)

// A chain of tipsets comprising a base (the last finalised tipset from which the chain extends).
// and (possibly empty) suffix.
// Tipsets are assumed to be built contiguously on each other,
// though epochs may be missing due to null rounds.
// The zero value is not a valid chain, and represents a "bottom" value
// when used in a Granite message.
type ECChain struct {
	TipSets []*TipSet

	key           ECChainKey `cborgen:"ignore"`
	keyLazyLoader sync.Once  `cborgen:"ignore"`
}

func (c *ECChain) UnmarshalJSON(i []byte) error {
	return json.Unmarshal(i, &c.TipSets)
}

func (c *ECChain) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.TipSets)
}

func (c *ECChain) UnmarshalCBOR(r io.Reader) error {
	// Unmarshall as []TipSet for backward compatibility.
	chain := LegacyECChain{}
	if err := chain.UnmarshalCBOR(r); err != nil {
		return err
	}
	if length := len(chain); length > 0 {
		*c = ECChain{}
		c.TipSets = make([]*TipSet, length)
		for i := range length {
			c.TipSets[i] = &chain[i]
		}
	}
	return nil
}

func (c *ECChain) MarshalCBOR(w io.Writer) error {
	// Marshall as []TipSet for backward compatibility.
	chain := LegacyECChain{}
	if length := c.Len(); length > 0 {
		chain = make([]TipSet, length)
		for i := range length {
			chain[i] = *c.TipSets[i]
		}
	}
	return chain.MarshalCBOR(w)
}

// A map key for a chain. The zero value means "bottom".
type ECChainKey merkle.Digest

func (k ECChainKey) IsZero() bool { return k == merkle.ZeroDigest }

// Creates a new chain.
func NewChain(base *TipSet, suffix ...*TipSet) (*ECChain, error) {
	var chain ECChain
	chain.TipSets = append(make([]*TipSet, 0, len(suffix)+1), base)
	chain.TipSets = append(chain.TipSets, suffix...)
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	return &chain, nil
}

// IsZero checks whether the chain is zero. A chain is zero if it has no tipsets
// or is nil.
func (c *ECChain) IsZero() bool {
	return c == nil || len(c.TipSets) == 0
}

func (c *ECChain) HasSuffix() bool {
	return c.Len() > 1
}

// Returns the base tipset, nil if the chain is zero.
func (c *ECChain) Base() *TipSet {
	if c.IsZero() {
		return nil
	}
	return c.TipSets[0]
}

// Returns the suffix of the chain after the base.
//
// Returns nil if the chain is zero or base.
func (c *ECChain) Suffix() []*TipSet {
	if c.IsZero() {
		return nil
	}
	return c.TipSets[1:]
}

// Returns the last tipset in the chain.
// This could be the base tipset if there is no suffix.
//
// Returns nil if the chain is zero.
func (c *ECChain) Head() *TipSet {
	if c.IsZero() {
		return nil
	}
	return c.TipSets[c.Len()-1]
}

// Returns a new chain with the same base and no suffix.
//
// Returns nil if the chain is zero.
func (c *ECChain) BaseChain() *ECChain {
	if c.IsZero() {
		return nil
	}
	return &ECChain{
		TipSets: []*TipSet{c.TipSets[0]},
	}
}

// Extend the chain with the given tipsets, returning the new chain.
//
// Panics if the chain is zero.
func (c *ECChain) Extend(tips ...TipSetKey) *ECChain {
	offset := c.Head().Epoch + 1
	pt := c.Head().PowerTable
	var extended ECChain
	extended.TipSets = make([]*TipSet, 0, c.Len()+len(tips))
	extended.TipSets = append(extended.TipSets, c.TipSets...)
	for i, tip := range tips {
		extended.TipSets = append(extended.TipSets, &TipSet{
			Epoch:      offset + int64(i),
			Key:        tip,
			PowerTable: pt,
		})
	}
	return &extended
}

// Returns a chain with suffix (after the base) truncated to a maximum length.
// Prefix(0) returns the base chain.
//
// Returns the zero chain if the chain is zero.
func (c *ECChain) Prefix(to int) *ECChain {
	if c.IsZero() {
		return nil
	}
	length := min(to+1, c.Len())
	var prefix ECChain
	// truncate capacity so appending to this chain won't modify the shared slice.
	prefix.TipSets = c.TipSets[:length:length]
	return &prefix
}

// Eq Compares two ECChains for equality.
// Note that two zero chains are considered equal. See IsZero.
func (c *ECChain) Eq(other *ECChain) bool {
	if oneZero, otherZero := c.IsZero(), other.IsZero(); oneZero || otherZero {
		return oneZero == otherZero
	}
	if c.Len() != other.Len() {
		return false
	}
	for i := range c.TipSets {
		if !c.TipSets[i].Equal(other.TipSets[i]) {
			return false
		}
	}
	return true
}

// Check whether a chain has a specific base tipset.
//
// Always false for a zero value.
func (c *ECChain) HasBase(t *TipSet) bool {
	return t != nil && !c.IsZero() && c.Base().Equal(t)
}

// Validates a chain value, returning an error if it finds any issues.
// A chain is valid if it meets the following criteria:
// 1) All contained tipsets are non-empty.
// 2) All epochs are >= 0 and increasing.
// 3) The chain is not longer than ChainMaxLen.
// An entirely zero-valued chain itself is deemed valid. See ECChain.IsZero.
func (c *ECChain) Validate() error {
	if c.IsZero() {
		return nil
	}
	if c.Len() > ChainMaxLen {
		return errors.New("chain too long")
	}
	var lastEpoch int64 = -1
	for i, ts := range c.TipSets {
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

// HasPrefix checks whether a chain has the given chain as a prefix, including
// base. This function always returns if either chain is zero.
func (c *ECChain) HasPrefix(other *ECChain) bool {
	if c.IsZero() || other.IsZero() {
		return false
	}
	if other.Len() > c.Len() {
		return false
	}
	for i := range other.TipSets {
		if !c.TipSets[i].Equal(other.TipSets[i]) {
			return false
		}
	}
	return true
}

func (c *ECChain) Len() int {
	if c == nil {
		return 0
	}
	return len(c.TipSets)
}

// Returns an identifier for the chain suitable for use as a map key.
// This must completely determine the sequence of tipsets in the chain.
func (c *ECChain) Key() ECChainKey {
	if c.IsZero() {
		return merkle.ZeroDigest
	}
	c.keyLazyLoader.Do(func() {
		values := make([][]byte, c.Len())
		for i, ts := range c.TipSets {
			values[i] = ts.MarshalForSigning()
		}
		c.key = merkle.Tree(values)
	})
	return c.key
}

func (c *ECChain) String() string {
	if c.IsZero() {
		return "丄"
	}
	var b strings.Builder
	b.WriteString("[")

	chainLength := c.Len()
	for i, ts := range c.TipSets {
		b.WriteString(ts.String())
		if i < chainLength-1 {
			b.WriteString(", ")
		}
		if b.Len() > 77 {
			b.WriteString("...")
			break
		}
	}
	b.WriteString("]")
	b.WriteString(fmt.Sprintf("len(%d)", chainLength))
	return b.String()
}

func (c *ECChain) Append(suffix ...*TipSet) *ECChain {
	var prefix []*TipSet
	if c != nil {
		prefix = c.TipSets
	}
	return &ECChain{
		TipSets: append(prefix, suffix...),
	}
}
