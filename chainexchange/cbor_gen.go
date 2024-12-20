// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package chainexchange

import (
	"fmt"
	"io"
	"math"
	"sort"

	gpbft "github.com/filecoin-project/go-f3/gpbft"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

var lengthBufMessage = []byte{130}

func (t *Message) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write(lengthBufMessage); err != nil {
		return err
	}

	// t.Instance (uint64) (uint64)

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Instance)); err != nil {
		return err
	}

	// t.Chain (gpbft.ECChain) (slice)
	if len(t.Chain) > 8192 {
		return xerrors.Errorf("Slice value in field t.Chain was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(len(t.Chain))); err != nil {
		return err
	}
	for _, v := range t.Chain {
		if err := v.MarshalCBOR(cw); err != nil {
			return err
		}

	}
	return nil
}

func (t *Message) UnmarshalCBOR(r io.Reader) (err error) {
	*t = Message{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Instance (uint64) (uint64)

	{

		maj, extra, err = cr.ReadHeader()
		if err != nil {
			return err
		}
		if maj != cbg.MajUnsignedInt {
			return fmt.Errorf("wrong type for uint64 field")
		}
		t.Instance = uint64(extra)

	}
	// t.Chain (gpbft.ECChain) (slice)

	maj, extra, err = cr.ReadHeader()
	if err != nil {
		return err
	}

	if extra > 8192 {
		return fmt.Errorf("t.Chain: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Chain = make([]gpbft.TipSet, extra)
	}

	for i := 0; i < int(extra); i++ {
		{
			var maj byte
			var extra uint64
			var err error
			_ = maj
			_ = extra
			_ = err

			{

				if err := t.Chain[i].UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Chain[i]: %w", err)
				}

			}

		}
	}
	return nil
}
