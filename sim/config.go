package sim

import (
	"time"

	"github.com/filecoin-project/go-f3/sim/signing"
)

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount int
	LatencySeed int64
	// Mean delivery latency for messages.
	LatencyMean time.Duration
	// Duration of EC epochs.
	ECEpochDuration time.Duration
	// Time to wait after EC epoch before starting next instance.
	ECStabilisationDelay time.Duration
	// If nil then FakeSigningBackend is used unless overridden by F3_TEST_USE_BLS
	SigningBacked signing.Backend
}

func (c Config) UseBLS() Config {
	c.SigningBacked = signing.NewBLSBackend()
	return c
}
