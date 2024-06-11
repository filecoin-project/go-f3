package latency

import (
	"math"
	"math/rand"
	"sync"
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

	// latencyFromToLock protects concurrent access to latencyFromTo map.
	latencyFromToLock sync.Mutex
	latencyFromTo     map[gpbft.ActorID]map[gpbft.ActorID]time.Duration
}

// NewLogNormal instantiates a new latency model of log normal latency
// distribution with the given mean. This model will always return zero if mean
// latency duration is less than or equal to zero.
func NewLogNormal(seed int64, mean time.Duration) *LogNormal {
	return &LogNormal{
		rng:           rand.New(rand.NewSource(seed)),
		mean:          mean,
		latencyFromTo: make(map[gpbft.ActorID]map[gpbft.ActorID]time.Duration),
	}
}

// Sample returns latency samples that correspond to the log normal distribution
// with the configured mean. The samples returned disregard time and
// participants, i.e. all the samples returned correspond to a fixed log normal
// distribution. Latency from one participant to another may be asymmetric and
// once generated remains constant for the lifetime of a simulation.
//
// Note, when mean configured latency is not larger than zero the latency sample will
// always be zero.
func (l *LogNormal) Sample(_ time.Time, from gpbft.ActorID, to gpbft.ActorID) time.Duration {
	if l.mean <= 0 {
		return 0
	}

	l.latencyFromToLock.Lock()
	defer l.latencyFromToLock.Unlock()

	latencyFrom, latencyFromFound := l.latencyFromTo[from]
	if !latencyFromFound {
		latencyFrom = make(map[gpbft.ActorID]time.Duration)
		l.latencyFromTo[from] = latencyFrom
	}
	latencyTo, latencyToFound := latencyFrom[to]
	if !latencyToFound {
		latencyTo = l.generate()
		latencyFrom[to] = latencyTo
	}
	return latencyTo
}

func (l *LogNormal) generate() time.Duration {
	norm := l.rng.NormFloat64()
	lognorm := math.Exp(norm)
	return time.Duration(lognorm * float64(l.mean))
}
