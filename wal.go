package f3

import (
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/writeaheadlog"
)

type walEntry struct {
	gpbft.GMessage
}

var _ = (*writeaheadlog.Entry)(nil)

func (we *walEntry) WALEpoch() uint64 {
	return we.Vote.Instance
}
