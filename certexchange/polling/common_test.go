package polling_test

import (
	"github.com/filecoin-project/go-f3/gpbft"

	logging "github.com/ipfs/go-log/v2"
)

const testNetworkName gpbft.NetworkName = "testnet"

var log = logging.Logger("certexchange-poller-test")
