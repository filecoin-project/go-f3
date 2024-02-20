package sim

import (
	"math"
	"math/rand"
	"time"
)

// A model for network latency.
type LatencyModel interface {
	Sample() time.Duration
}

type LogNormalLatency struct {
	rng  *rand.Rand
	mean time.Duration
}

func NewLogNormal(seed int64, mean time.Duration) *LogNormalLatency {
	rng := rand.New(rand.NewSource(seed))
	return &LogNormalLatency{rng: rng, mean: mean}
}

func (l *LogNormalLatency) Sample() time.Duration {
	norm := l.rng.NormFloat64()
	lognorm := math.Exp(norm)
	return time.Duration(lognorm * float64(l.mean))
}
