package f3

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Manifest struct {
	NetworkName       gpbft.NetworkName
	InitialInstance   uint64
	InitialPowerTable gpbft.PowerEntries
	BootstrapEpoch    int64

	ECFinality int64
	// The delay after a tipset is produced before we attempt to finalize it.
	ECDelay time.Duration
	// The delay between tipsets.
	ECPeriod         time.Duration
	CommiteeLookback uint64

	//Temporary
	ECBoostrapTimestamp time.Time
}

func LocalnetManifest() Manifest {
	rng := make([]byte, 4)
	_, _ = rand.Read(rng)
	m := Manifest{
		NetworkName:      gpbft.NetworkName(fmt.Sprintf("localnet-%X", rng)),
		BootstrapEpoch:   1000,
		ECFinality:       900,
		CommiteeLookback: 5,
		ECDelay:          60 * time.Second,

		ECPeriod: 30 * time.Second,
	}
	m.ECBoostrapTimestamp = time.Now().Add(-time.Duration(m.BootstrapEpoch) * m.ECPeriod)
	return m
}
