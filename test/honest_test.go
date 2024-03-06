package test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

///// Tests with no adversaries.

func TestSingleton(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(1), GraniteConfig(), sim.TraceNone)
	a := sm.Base().Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a})

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestSyncPair(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2), GraniteConfig(), sim.TraceNone)
	a := sm.Base().Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestSyncPairBLS(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2).UseBLS(), GraniteConfig(), sim.TraceNone)
	a := sm.Base().Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestASyncPair(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		//fmt.Println("i =", i)
		sm := sim.NewSimulation(newAsyncConfig(2, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		expectDecision(t, sm, a.Head(), sm.Base().Head())
	}
}

func TestSyncPairDisagree(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2), GraniteConfig(), sim.TraceNone)
	a := sm.Base().Extend(sm.CIDGen.Sample())
	b := sm.Base().Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	// Decide base chain as the only common value.
	expectDecision(t, sm, sm.Base().Head())
}

func TestSyncPairDisagreeBLS(t *testing.T) {
	sm := sim.NewSimulation(newSyncConfig(2).UseBLS(), GraniteConfig(), sim.TraceNone)
	a := sm.Base().Extend(sm.CIDGen.Sample())
	b := sm.Base().Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	// Decide base chain as the only common value.
	expectDecision(t, sm, sm.Base().Head())
}

func TestAsyncPairDisagree(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		//fmt.Println("i =", i)
		sm := sim.NewSimulation(newAsyncConfig(2, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		b := sm.Base().Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		// Decide base chain as the only common value.
		expectDecision(t, sm, sm.Base().Head())
	}
}

func TestSyncAgreement(t *testing.T) {
	for n := 3; n <= 50; n++ {
		sm := sim.NewSimulation(newSyncConfig(n), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})
		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		expectDecision(t, sm, a.Head())
	}
}

func TestAsyncAgreement(t *testing.T) {
	t.Parallel()
	// These iterations are much slower, so we can't test as many participants.
	for n := 3; n <= 16; n++ {
		for i := 0; i < ASYNC_ITERS; i++ {
			//fmt.Println("n =", n, "i =", i)
			sm := sim.NewSimulation(newAsyncConfig(n, i), GraniteConfig(), sim.TraceNone)
			a := sm.Base().Extend(sm.CIDGen.Sample())
			sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

			require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
			expectDecision(t, sm, sm.Base().Head(), a.Head())
		}
	}
}

func TestSyncHalves(t *testing.T) {
	for n := 4; n <= 50; n += 2 {
		sm := sim.NewSimulation(newSyncConfig(n), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		b := sm.Base().Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		expectDecision(t, sm, sm.Base().Head())
	}
}

func TestSyncHalvesBLS(t *testing.T) {
	for n := 4; n <= 10; n += 2 {
		sm := sim.NewSimulation(newSyncConfig(n).UseBLS(), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		b := sm.Base().Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		expectDecision(t, sm, sm.Base().Head())
	}
}

func TestAsyncHalves(t *testing.T) {
	t.Parallel()
	for n := 4; n <= 2; n += 2 {
		for i := 0; i < ASYNC_ITERS; i++ {
			sm := sim.NewSimulation(newAsyncConfig(n, i), GraniteConfig(), sim.TraceNone)
			a := sm.Base().Extend(sm.CIDGen.Sample())
			b := sm.Base().Extend(sm.CIDGen.Sample())
			sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

			require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
			// Groups split 50/50 always decide the base.
			expectDecision(t, sm, sm.Base().Head())
		}
	}
}

func TestRequireStrongQuorumToProgress(t *testing.T) {
	t.Parallel()
	for i := 0; i < ASYNC_ITERS; i++ {
		sm := sim.NewSimulation(newAsyncConfig(30, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base().Extend(sm.CIDGen.Sample())
		b := sm.Base().Extend(sm.CIDGen.Sample())
		// No strict > quorum.
		sm.SetChains(sim.ChainCount{Count: 20, Chain: a}, sim.ChainCount{Count: 10, Chain: b})

		require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
		// Must decide base.
		expectDecision(t, sm, sm.Base().Head())
	}
}

func TestLongestCommonPrefix(t *testing.T) {
	// This test uses a synchronous configuration to ensure timely message delivery.
	// If async, it is possible to decide the base chain if QUALITY messages are delayed.
	sm := sim.NewSimulation(newSyncConfig(4), GraniteConfig(), sim.TraceAll)
	ab := sm.Base().Extend(sm.CIDGen.Sample())
	abc := ab.Extend(sm.CIDGen.Sample())
	abd := ab.Extend(sm.CIDGen.Sample())
	abe := ab.Extend(sm.CIDGen.Sample())
	abf := ab.Extend(sm.CIDGen.Sample())
	sm.SetChains(
		sim.ChainCount{Count: 1, Chain: abc},
		sim.ChainCount{Count: 1, Chain: abd},
		sim.ChainCount{Count: 1, Chain: abe},
		sim.ChainCount{Count: 1, Chain: abf},
	)

	require.NoErrorf(t, sm.Run(MAX_ROUNDS), "%s", sm.Describe())
	// Must decide ab, the longest common prefix.
	expectDecision(t, sm, ab.Head())
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

func expectDecision(t *testing.T, sm *sim.Simulation, expected ...gpbft.TipSet) {
	decision, ok := sm.GetDecision(0, 0)
	require.True(t, ok, "no decision")
	for _, e := range expected {
		if decision.Head() == e {
			return
		}
	}
	require.Fail(t, fmt.Sprintf("decided %s, expected one of %s", decision, expected))
}
