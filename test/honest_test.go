package test

import (
	"github.com/anorth/f3sim/net"
	"github.com/anorth/f3sim/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

///// Tests with no adversaries.

func TestSingleton(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(1), GraniteConfig(), net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{1, *a})

	require.True(t, sm.Run(MAX_ROUNDS))
	expectRoundDecision(t, sm, 0, a.Head())
}

func TestSyncPair(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2), GraniteConfig(), net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

	require.True(t, sm.Run(MAX_ROUNDS))
	expectRoundDecision(t, sm, 0, a.Head())
}

func TestASyncPair(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		sm := sim.NewSimulation(newAsyncConfig(2, i), GraniteConfig(), net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

		require.True(t, sm.Run(MAX_ROUNDS))
		// Can't guarantee progress when async.
		expectEventualDecision(t, sm, a.Head(), sm.Base.Head())
	}
}

func TestSyncPairDisagree(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2), GraniteConfig(), net.TraceNone)
	a := sm.Base.Extend(sm.CIDGen.Sample())
	b := sm.Base.Extend(sm.CIDGen.Sample())
	sm.ReceiveChains(sim.ChainCount{1, *a}, sim.ChainCount{1, *b})

	require.True(t, sm.Run(MAX_ROUNDS))
	// Decide base chain as the only common value, even when synchronous.
	expectRoundDecision(t, sm, 0, sm.Base.Head())
}

func TestAsyncPairDisagree(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		sm := sim.NewSimulation(newAsyncConfig(2, i), GraniteConfig(), net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		b := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{1, *a}, sim.ChainCount{1, *b})

		require.True(t, sm.Run(MAX_ROUNDS))
		// Decide base chain as the only common value.
		expectRoundDecision(t, sm, 0, sm.Base.Head())
	}
}

func TestSyncAgreement(t *testing.T) {
	for n := 3; n <= 50; n++ {
		sm := sim.NewSimulation(newSyncConfig(n), GraniteConfig(), net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})
		require.True(t, sm.Run(MAX_ROUNDS))
		// Synchronous, agreeing groups always decide the candidate.
		expectRoundDecision(t, sm, 0, a.Head())
	}
}

func TestAsyncAgreement(t *testing.T) {
	t.Parallel()
	// These iterations are much slower, so we can't test as many participants.
	for n := 3; n <= 16; n++ {
		for i := 0; i < ASYNC_ITERS; i++ {
			//fmt.Println("n =", n, "i =", i)
			sm := sim.NewSimulation(newAsyncConfig(n, i), GraniteConfig(), net.TraceNone)
			a := sm.Base.Extend(sm.CIDGen.Sample())
			sm.ReceiveChains(sim.ChainCount{len(sm.Participants), *a})

			require.True(t, sm.Run(MAX_ROUNDS))
			// Can't guarantee progress when async.
			expectEventualDecision(t, sm, sm.Base.Head(), a.Head())
		}
	}
}

func TestSyncHalves(t *testing.T) {
	for n := 4; n <= 50; n += 2 {
		sm := sim.NewSimulation(newSyncConfig(n), GraniteConfig(), net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		b := sm.Base.Extend(sm.CIDGen.Sample())
		sm.ReceiveChains(sim.ChainCount{n / 2, *a}, sim.ChainCount{n / 2, *b})

		require.True(t, sm.Run(MAX_ROUNDS))
		// Groups split 50/50 always decide the base.
		expectRoundDecision(t, sm, 0, sm.Base.Head())
	}
}

func TestAsyncHalves(t *testing.T) {
	t.Parallel()
	for n := 4; n <= 2; n += 2 {
		for i := 0; i < ASYNC_ITERS; i++ {
			sm := sim.NewSimulation(newAsyncConfig(n, i), GraniteConfig(), net.TraceNone)
			a := sm.Base.Extend(sm.CIDGen.Sample())
			b := sm.Base.Extend(sm.CIDGen.Sample())
			sm.ReceiveChains(sim.ChainCount{n / 2, *a}, sim.ChainCount{n / 2, *b})

			require.True(t, sm.Run(MAX_ROUNDS))
			// Groups split 50/50 always decide the base.
			expectRoundDecision(t, sm, 0, sm.Base.Head())
		}
	}
}

func TestRequireStrongQuorumToProgress(t *testing.T) {
	t.Parallel()
	for i := 0; i < ASYNC_ITERS; i++ {
		sm := sim.NewSimulation(newAsyncConfig(30, i), GraniteConfig(), net.TraceNone)
		a := sm.Base.Extend(sm.CIDGen.Sample())
		b := sm.Base.Extend(sm.CIDGen.Sample())
		// No strict > quorum.
		sm.ReceiveChains(sim.ChainCount{20, *a}, sim.ChainCount{10, *b})

		require.True(t, sm.Run(MAX_ROUNDS))
		// Must decide base, but can't tell which round.
		expectEventualDecision(t, sm, sm.Base.Head())
	}
}

func newSyncConfig(honestCount int) sim.Config {
	return sim.Config{
		HonestCount: honestCount,
		LatencySeed: 0,
		LatencyMean: LATENCY_SYNC,
	}
}

func newAsyncConfig(honestCount int, latencySeed int) sim.Config {
	return sim.Config{
		HonestCount: honestCount,
		LatencySeed: int64(latencySeed),
		LatencyMean: LATENCY_ASYNC,
	}
}

func expectRoundDecision(t *testing.T, sm *sim.Simulation, expectedRound int, expected ...*net.TipSet) {
	decision, round := sm.Participants[0].Finalised()
	require.Equal(t, expectedRound, round)

	for _, e := range expected {
		if decision.CID == e.CID {
			return
		}
	}
	require.Fail(t, "decided %s, expected one of %s", decision, expected)
}

func expectEventualDecision(t *testing.T, sm *sim.Simulation, expected ...*net.TipSet) {
	decision, _ := sm.Participants[0].Finalised()
	for _, e := range expected {
		if decision.CID == e.CID {
			return
		}
	}
	require.Fail(t, "decided %s, expected one of %s", decision, expected)
}
