package writeaheadlog

import (
	"io"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Entry = (*MessageEntry)(nil)

type MessageEntry struct {
	Message *gpbft.GMessage
}

func (we *MessageEntry) WALEpoch() uint64 {
	return we.Message.Vote.Instance
}

func (we *MessageEntry) MarshalCBOR(w io.Writer) error {
	return we.Message.MarshalCBOR(w)
}

func (we *MessageEntry) UnmarshalCBOR(r io.Reader) error {
	we.Message = &gpbft.GMessage{}
	return we.Message.UnmarshalCBOR(r)
}
