package test

import (
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

///// Tests with no adversaries.

func TestSingleton(t *testing.T) {
	sm := sim.NewSimulation(&sim.Config{
		HonestCount:  1,
		LatencySeed:  0,
		LatencyMean:  0,
		GraniteDelta: 0.200,
	}, net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{1, *a})

	require.True(t, sm.Run())
	require.Equal(t, *a.Head(), sm.Participants[0].Finalised())
}

func TestSyncPair(t *testing.T) {
	sm := sim.NewSimulation(&sim.Config{
		HonestCount:  2,
		LatencySeed:  0,
		LatencyMean:  0, // Synchronous
		GraniteDelta: 0.200,
	}, net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

	require.True(t, sm.Run())
	require.Equal(t, *a.Head(), sm.Participants[0].Finalised())
}

func TestASyncPair(t *testing.T) {
	sm := sim.NewSimulation(&sim.Config{
		HonestCount:  2,
		LatencySeed:  0,
		LatencyMean:  100, // Async
		GraniteDelta: 0.200,
	}, net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

	require.True(t, sm.Run())
	require.Equal(t, *sm.Base.Head(), sm.Participants[0].Finalised())
}

func TestSyncPairDisagree(t *testing.T) {
	sm := sim.NewSimulation(&sim.Config{
		HonestCount:  2,
		LatencySeed:  0,
		LatencyMean:  0, // Sync
		GraniteDelta: 0.200,
	}, net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	b := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{1, *a}, sim.ChainCount{1, *b})

	require.True(t, sm.Run())
	// Decide base chain as the only common value, even when synchronous.
	require.Equal(t, *sm.Base.Head(), sm.Participants[0].Finalised())
}

func TestSyncAgreement(t *testing.T) {
	for n := 3; n <= 50; n++ {
		sm := sim.NewSimulation(&sim.Config{
			HonestCount:  n,
			LatencySeed:  0,
			LatencyMean:  0, // Synchronous
			GraniteDelta: 0.200,
		}, net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})
		require.True(t, sm.Run())
		// Synchronous, agreeing groups always decide the candidate.
		require.Equal(t, *a.Head(), sm.Participants[0].Finalised())
	}
}

func TestAsyncAgreement(t *testing.T) {
	for i := 0; i < 1000; i++ {
		sm := sim.NewSimulation(&sim.Config{
			HonestCount:  3,
			LatencySeed:  int64(i),
			LatencyMean:  0.100,
			GraniteDelta: 0.200,
		}, net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

		require.True(t, sm.Run())
		// We can't assert which of the base or candidate is decided, as network latency
		// may prevent candidate getting enough initial power.
	}
}

func TestSyncHalves(t *testing.T) {
	for n := 4; n <= 50; n += 2 {
		sm := sim.NewSimulation(&sim.Config{
			HonestCount:  n,
			LatencySeed:  0,
			LatencyMean:  0, // Synchronous
			GraniteDelta: 0.200,
		}, net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		b := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{n / 2, *a}, sim.ChainCount{n / 2, *b})

		require.True(t, sm.Run())
		// Synchronous groups split 50/50 always decide the base.
		require.Equal(t, *sm.Base.Head(), sm.Participants[0].Finalised())
	}
}

func TestAsyncHalves(t *testing.T) {
	for i := 0; i < 1000; i++ {
		sm := sim.NewSimulation(&sim.Config{
			HonestCount:  4,
			LatencySeed:  int64(i),
			LatencyMean:  0.100,
			GraniteDelta: 0.200,
		}, net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		b := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{2, *a}, sim.ChainCount{2, *b})

		require.True(t, sm.Run())
		require.Equal(t, *sm.Base.Head(), sm.Participants[0].Finalised())
	}
}
