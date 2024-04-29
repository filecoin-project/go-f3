package latency

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_ Model = (*none)(nil)

	// None represents zero no-op latency model.
	None = none{}
)

// None represents zero latency model.
type none struct{}

func (l none) Sample(time.Time, gpbft.ActorID, gpbft.ActorID) time.Duration { return 0 }
