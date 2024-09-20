package f3

import (
	"io"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/writeaheadlog"
)

type walEntry struct {
	Message *gpbft.GMessage
}

var _ = (*writeaheadlog.Entry)(nil)

func (we *walEntry) WALEpoch() uint64 {
	return we.Message.Vote.Instance
}

func (we *walEntry) MarshalCBOR(w io.Writer) error {
	return we.Message.MarshalCBOR(w)
}

func (we *walEntry) UnmarshalCBOR(r io.Reader) error {
	we.Message = &gpbft.GMessage{}
	return we.Message.UnmarshalCBOR(r)
}
