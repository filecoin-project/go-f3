package latency

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Model = (*LogNormal)(nil)

// LogNormal represents a log normal latency distribution with a configurable
// mean latency. This latency model does not specialise based on host clock time
// nor participants.
type LogNormal struct {
	rng  *rand.Rand
	mean time.Duration
}

// NewLogNormal instantiates a new latency model of log normal latency
// distribution with the given mean.
func NewLogNormal(seed int64, mean time.Duration) (*LogNormal, error) {
	if mean < 0 {
		return nil, errors.New("mean duration cannot be negative")
	}
	return &LogNormal{rng: rand.New(rand.NewSource(seed)), mean: mean}, nil
}

// Sample returns latency samples that correspond to the log normal distribution
// with the configured mean. The samples returned disregard time and
// participants, i.e. all the samples returned correspond to a fixed log normal
// distribution.
//
// Note, here from and to are the same the latency sample will always be zero.
func (l *LogNormal) Sample(_ time.Time, from gpbft.ActorID, to gpbft.ActorID) time.Duration {
	if from == to {
		return 0
	}
	norm := l.rng.NormFloat64()
	lognorm := math.Exp(norm)
	return time.Duration(lognorm * float64(l.mean))
}
