package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

///// Tests for a single instance with no adversaries.

func TestSingleton(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(1), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a})

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestSyncPair(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(2), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestSyncPairBLS(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(2).UseBLS(), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	expectDecision(t, sm, a.Head())
}

func TestASyncPair(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		//fmt.Println("i =", i)
		sm := sim.NewSimulation(AsyncConfig(2, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
		expectDecision(t, sm, a.Head(), sm.Base(0).Head())
	}
}

func TestSyncPairDisagree(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(2), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	b := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	// Decide base chain as the only common value.
	expectDecision(t, sm, sm.Base(0).Head())
}

func TestSyncPairDisagreeBLS(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(2).UseBLS(), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	b := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	// Decide base chain as the only common value.
	expectDecision(t, sm, sm.Base(0).Head())
}

func TestAsyncPairDisagree(t *testing.T) {
	for i := 0; i < ASYNC_ITERS; i++ {
		//fmt.Println("i =", i)
		sm := sim.NewSimulation(AsyncConfig(2, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		b := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: 1, Chain: a}, sim.ChainCount{Count: 1, Chain: b})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
		// Decide base chain as the only common value.
		expectDecision(t, sm, sm.Base(0).Head())
	}
}

func TestSyncAgreement(t *testing.T) {
	for n := 3; n <= 50; n++ {
		sm := sim.NewSimulation(SyncConfig(n), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})
		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
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
			sm := sim.NewSimulation(AsyncConfig(n, i), GraniteConfig(), sim.TraceNone)
			a := sm.Base(0).Extend(sm.CIDGen.Sample())
			sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

			require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
			expectDecision(t, sm, sm.Base(0).Head(), a.Head())
		}
	}
}

func TestSyncHalves(t *testing.T) {
	for n := 4; n <= 50; n += 2 {
		sm := sim.NewSimulation(SyncConfig(n), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		b := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		expectDecision(t, sm, sm.Base(0).Head())
	}
}

func TestSyncHalvesBLS(t *testing.T) {
	for n := 4; n <= 10; n += 2 {
		sm := sim.NewSimulation(SyncConfig(n).UseBLS(), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		b := sm.Base(0).Extend(sm.CIDGen.Sample())
		sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
		// Groups split 50/50 always decide the base.
		expectDecision(t, sm, sm.Base(0).Head())
	}
}

func TestAsyncHalves(t *testing.T) {
	t.Parallel()
	for n := 4; n <= 20; n += 2 {
		for i := 0; i < ASYNC_ITERS; i++ {
			sm := sim.NewSimulation(AsyncConfig(n, i), GraniteConfig(), sim.TraceNone)
			a := sm.Base(0).Extend(sm.CIDGen.Sample())
			b := sm.Base(0).Extend(sm.CIDGen.Sample())
			sm.SetChains(sim.ChainCount{Count: n / 2, Chain: a}, sim.ChainCount{Count: n / 2, Chain: b})

			require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
			// Groups split 50/50 always decide the base.
			expectDecision(t, sm, sm.Base(0).Head())
		}
	}
}

func TestRequireStrongQuorumToProgress(t *testing.T) {
	t.Parallel()
	for i := 0; i < ASYNC_ITERS; i++ {
		sm := sim.NewSimulation(AsyncConfig(30, i), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		b := sm.Base(0).Extend(sm.CIDGen.Sample())
		// No strict > quorum.
		sm.SetChains(sim.ChainCount{Count: 20, Chain: a}, sim.ChainCount{Count: 10, Chain: b})

		require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
		// Must decide base.
		expectDecision(t, sm, sm.Base(0).Head())
	}
}

func TestLongestCommonPrefix(t *testing.T) {
	// This test uses a synchronous configuration to ensure timely message delivery.
	// If async, it is possible to decide the base chain if QUALITY messages are delayed.
	sm := sim.NewSimulation(SyncConfig(4), GraniteConfig(), sim.TraceNone)
	ab := sm.Base(0).Extend(sm.CIDGen.Sample())
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

	require.NoErrorf(t, sm.Run(1, MAX_ROUNDS), "%s", sm.Describe())
	// Must decide ab, the longest common prefix.
	expectDecision(t, sm, ab.Head())
}
