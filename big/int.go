package big

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// BigIntMaxSerializedLen is the max length of a byte slice representing a CBOR serialized big.
const BigIntMaxSerializedLen = 128

type Int big.Int

func NewInt(i int64) *Int {
	return (*Int)(big.NewInt(i))
}

func (i *Int) int() *big.Int {
	return (*big.Int)(i)
}

func FromString(s string) (*Int, error) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse string as a big int")
	}

	return (*Int)(v), nil
}

func (bi *Int) Copy() *Int {
	return (*Int)(new(big.Int).Set(bi.int()))
}

func Mul(a, b *Int) *Int {
	return (*Int)(new(big.Int).Mul(a.int(), b.int()))
}

func (bi *Int) MulAssign(o *Int) {
	bi.int().Mul(bi.int(), o.int())
}

func Div(a, b *Int) *Int {
	return (*Int)(new(big.Int).Div(a.int(), b.int()))
}

func (bi *Int) DivAssign(o *Int) {
	bi.int().Div(bi.int(), o.int())
}

func Mod(a, b *Int) *Int {
	return (*Int)(new(big.Int).Mod(a.int(), b.int()))
}

func (bi *Int) ModAssign(o *Int) {
	bi.int().Mod(bi.int(), o.int())
}

func Add(a, b *Int) *Int {
	return (*Int)(new(big.Int).Add(a.int(), b.int()))
}

func (bi *Int) AddAssign(o *Int) {
	bi.int().Add(bi.int(), o.int())
}

func Sub(a, b *Int) *Int {
	return (*Int)(new(big.Int).Sub(a.int(), b.int()))
}

func (bi *Int) SubAssign(o *Int) {
	bi.int().Sub(bi.int(), o.int())
}

// Returns a**e unless e <= 0 (in which case returns 1).
func Exp(a *Int, e *Int) *Int {
	return (*Int)(new(big.Int).Exp(a.int(), e.int(), nil))
}

// Returns x << n
func Lsh(a *Int, n uint) *Int {
	return (*Int)(new(big.Int).Lsh(a.int(), n))
}

// Returns x >> n
func Rsh(a *Int, n uint) *Int {
	return (*Int)(new(big.Int).Rsh(a.int(), n))
}

func BitLen(a *Int) uint {
	return uint(a.int().BitLen())
}

func Cmp(a, b *Int) int {
	return a.Cmp(b)
}

func (bi *Int) Cmp(o *Int) int {
	return bi.int().Cmp(o.int())
}

// LessThan returns true if bi < o
func (bi *Int) LessThan(o *Int) bool {
	return bi.Cmp(o) < 0
}

// LessThanEqual returns true if bi <= o
func (bi *Int) LessThanEqual(o *Int) bool {
	return bi.Cmp(o) <= 0
}

// GreaterThan returns true if bi > o
func (bi *Int) GreaterThan(o *Int) bool {
	return bi.Cmp(o) > 0
}

// GreaterThanEqual returns true if bi >= o
func (bi *Int) GreaterThanEqual(o *Int) bool {
	return bi.Cmp(o) >= 0
}

// Neg returns the negative of bi.
func (bi *Int) Neg() *Int {
	return (*Int)(new(big.Int).Neg(bi.int()))
}

// Abs returns the absolute value of bi.
func (bi *Int) Abs() *Int {
	return (*Int)(new(big.Int).Abs(bi.int()))
}

// Equals returns true if bi == o
func (bi *Int) Equals(o *Int) bool {
	return bi.Cmp(o) == 0
}

func (bi *Int) MarshalJSON() ([]byte, error) {
	if bi == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(bi.int().String())
}

func (bi *Int) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	_, ok := bi.int().SetString(s, 10)
	if !ok {
		return fmt.Errorf("failed to parse big string: '%s'", string(b))
	}

	return nil
}

func (bi *Int) Bytes() ([]byte, error) {
	if bi == nil {
		return []byte{}, fmt.Errorf("failed to convert to bytes, big is nil")
	}

	switch {
	case bi.Sign() > 0:
		return append([]byte{0}, bi.int().Bytes()...), nil
	case bi.Sign() < 0:
		return append([]byte{1}, bi.int().Bytes()...), nil
	default: //  bi.Sign() == 0:
		return []byte{}, nil
	}
}

func (bi *Int) MarshalBinary() ([]byte, error) {
	return bi.Bytes()
}

func (bi *Int) UnmarshalBinary(buf []byte) error {
	*bi = Int{}
	if len(buf) == 0 {
		return nil
	}

	var negative bool
	switch buf[0] {
	case 0:
		negative = false
	case 1:
		negative = true
	default:
		return fmt.Errorf("big int prefix should be either 0 or 1, got %d", buf[0])
	}

	bi.int().SetBytes(buf[1:])
	if negative {
		bi.int().Neg(bi.int())
	}

	return nil
}

func (bi *Int) MarshalCBOR(w io.Writer) error {
	if bi == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	enc, err := bi.Bytes()
	if err != nil {
		return err
	}

	encLen := len(enc)
	if encLen > BigIntMaxSerializedLen {
		return fmt.Errorf("big integer byte array too long (%d bytes)", encLen)
	}

	header := cbg.CborEncodeMajorType(cbg.MajByteString, uint64(encLen))
	if _, err := w.Write(header); err != nil {
		return err
	}

	if _, err := w.Write(enc); err != nil {
		return err
	}

	return nil
}

func (bi *Int) UnmarshalCBOR(br io.Reader) error {
	*bi = Int{}

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if maj != cbg.MajByteString {
		return fmt.Errorf("cbor input for fil big int was not a byte string (%x)", maj)
	}

	if extra == 0 {
		return nil
	}

	if extra > BigIntMaxSerializedLen {
		return fmt.Errorf("big integer byte array too long (%d bytes)", extra)
	}

	buf := make([]byte, extra)
	if _, err := io.ReadFull(br, buf); err != nil {
		return err
	}

	return bi.UnmarshalBinary(buf)
}

func (i *Int) Sign() int {
	return i.int().Sign()
}

func (i *Int) String() string {
	return i.int().String()
}

func (i *Int) Format(f fmt.State, verb rune) {
	i.int().Format(f, verb)
}
