package f3

import (
	"time"

	"github.com/filecoin-project/go-f3/ec"
)

func computeTipsetTimestampAtEpoch(validTipset ec.TipSet, epoch int64, ECPeriod time.Duration) time.Time {
	// validTipset is base and epoch is the head
	// timestamp(head) = genesis + epoch(head) * period
	// timestamp(base) = genesis + epoch(base) * period + timestamp(head) - timestamp(head)
	// timestamp(base) = timestamp(head) + genesis + epoch(base) * period - genesis - epoch(head) * period
	// timestamp(base) = timestamp(head) + (epoch(base) - epoch(head)) * period
	return validTipset.Timestamp().Add(time.Duration(epoch-validTipset.Epoch()) * ECPeriod)
}
