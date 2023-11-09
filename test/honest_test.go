package test

import (
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

// Test simulation with three honest nodes
func TestHonest(t *testing.T) {
	for i := 0; i < 1000; i++ {
		simu := sim.NewSimulation(&sim.Config{
			ParticipantCount: 3,
			LatencySeed:      int64(i),
			LatencyMean:      0.100,
			GraniteDelta:     0.200,
		}, net.TraceNone)

		ok := simu.Run()
		require.True(t, ok)
	}
}
