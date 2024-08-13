package f3

import (
	"github.com/filecoin-project/go-f3/gpbft"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("f3")
var tracer gpbft.Tracer = (*gpbftTracer)(logging.WithSkip(logging.Logger("f3/gpbft"), 2))

// Tracer used by GPBFT, backed by a Zap logger.
type gpbftTracer logging.ZapEventLogger

// Log fulfills the gpbft.Tracer interface
func (h *gpbftTracer) Log(fmt string, args ...any) {
	(*logging.ZapEventLogger)(h).Debugf(fmt, args...)
}
