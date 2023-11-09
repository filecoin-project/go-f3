package test

import (
	"github.com/anorth/f3sim/adversary"
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAbsent(t *testing.T) {
	for i := 0; i < 1000; i++ {
		simu := sim.NewSimulation(&sim.Config{
			ParticipantCount: 4,
			AdversaryCount:   1,
			LatencySeed:      int64(i),
			LatencyMean:      0.100,
			GraniteDelta:     0.200,
		}, adversary.NewAbsent, net.TraceNone)

		ok := simu.Run()
		if !ok {
			simu.PrintResults()
		}
		require.True(t, ok)
	}
}
