package f3

import "github.com/filecoin-project/go-f3/gpbft"

type Manifest struct {
	NetworkName       gpbft.NetworkName
	InitialPowerTable gpbft.PowerEntries
	BootstrapEpoch    int64
}
