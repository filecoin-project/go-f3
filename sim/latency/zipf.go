package latency

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Model = (*Zipf)(nil)

// Zipf represents a log normal latency distribution with a configurable
// max latency. This latency model does not specialise based on host clock time
// nor participants.
type Zipf struct {
	dist *rand.Zipf
}

// NewZipf instantiates a new latency model of ZipF latency distribution with the
// given max.
func NewZipf(seed int64, s, v float64, max time.Duration) (*Zipf, error) {
	if max < 0 {
		return nil, errors.New("max duration cannot be negative")
	}
	dist := rand.NewZipf(rand.New(rand.NewSource(seed)), s, v, uint64(max))
	if dist == nil {
		return nil, fmt.Errorf("zipf parameters are out of band: s=%f, v=%f", s, v)
	}
	return &Zipf{dist: dist}, nil
}

// Sample returns latency samples that correspond to this ZipF numerical
// distribution. The samples returned disregard time and participants, i.e. the
// distribution does not vary over time nor for specific participants.
func (l *Zipf) Sample(time.Time, gpbft.ActorID, gpbft.ActorID) time.Duration {
	return time.Duration(l.dist.Uint64())
}
