package polling

import (
	"github.com/benbjohnson/clock"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("f3-certexchange")
var clk clock.Clock = clock.New()
