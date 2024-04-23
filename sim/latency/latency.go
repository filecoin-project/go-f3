package latency

import (
	"math"
	"math/rand"
	"time"
)

// A model for network latency.
type Model interface {
	Sample() time.Duration
}

type LogNormal struct {
	rng  *rand.Rand
	mean time.Duration
}

func NewLogNormal(seed int64, mean time.Duration) *LogNormal {
	rng := rand.New(rand.NewSource(seed))
	return &LogNormal{rng: rng, mean: mean}
}

func (l *LogNormal) Sample() time.Duration {
	norm := l.rng.NormFloat64()
	lognorm := math.Exp(norm)
	return time.Duration(lognorm * float64(l.mean))
}
