package net

import (
	"math"
	"math/rand"
)

// A model for network latency.
type LatencyModel interface {
	Sample() float64
}

type LogNormalLatency struct {
	rng  *rand.Rand
	mean float64
}

func NewLogNormal(rng *rand.Rand, mean float64) *LogNormalLatency {
	return &LogNormalLatency{rng: rng, mean: mean}
}

func (l *LogNormalLatency) Sample() float64 {
	norm := l.rng.NormFloat64()
	lognorm := math.Exp(norm)
	return lognorm * l.mean
}
