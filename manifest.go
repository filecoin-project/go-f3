package f3

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type Manifest struct {
	NetworkName       gpbft.NetworkName
	InitialPowerTable gpbft.PowerEntries
	BootstrapEpoch    int64

	ECFinality       int64
	ECDelay          time.Duration
	CommiteeLookback uint64

	//Temporary
	ECPeriod            time.Duration
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
		ECDelay:          30 * time.Second,

		ECPeriod: 30 * time.Second,
	}
	m.ECBoostrapTimestamp = time.Now().Add(-time.Duration(m.BootstrapEpoch) * m.ECPeriod)
	return m
}
