package certexchange

import (
	"math"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func FetchProtocolName(nn gpbft.NetworkName) protocol.ID {
	return protocol.ID("/f3/certexch/get/1/" + string(nn))
}

// Request unlimited certificates.
const NoLimit uint64 = math.MaxUint64

type Request struct {
	// First instance to fetch.
	FirstInstance uint64
	// Max number of instances to fetch. The server may respond with fewer certificates than
	// requested, even if more are available.
	Limit uint64
	// Include the full power table needed to validate the first finality certificate.
	// Checked by the user against their last finality certificate.
	IncludePowerTable bool
}

type ResponseHeader struct {
	// The next instance to be finalized. This is 0 when no instances have been finalized.
	PendingInstance uint64
	// Power table, if requested, or empty.
	PowerTable gpbft.PowerEntries
}
