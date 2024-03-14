package test

import (
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

///// Tests for multiple chained instances of the protocol, no adversaries.

const INSTANCE_COUNT = 4000

func TestMultiSingleton(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(1), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: 1, Chain: a})

	require.NoErrorf(t, sm.Run(INSTANCE_COUNT, MAX_ROUNDS), "%s", sm.Describe())
	expected := sm.EC.Instances[INSTANCE_COUNT].Base
	expectInstanceDecision(t, sm, INSTANCE_COUNT-1, expected.Head())
}

func TestMultiSyncPair(t *testing.T) {
	sm := sim.NewSimulation(SyncConfig(2), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(INSTANCE_COUNT, MAX_ROUNDS), "%s", sm.Describe())
	expected := sm.EC.Instances[INSTANCE_COUNT].Base
	expectInstanceDecision(t, sm, INSTANCE_COUNT-1, expected.Head())
}

func TestMultiASyncPair(t *testing.T) {
	sm := sim.NewSimulation(AsyncConfig(2, 0), GraniteConfig(), sim.TraceNone)
	a := sm.Base(0).Extend(sm.CIDGen.Sample())
	sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})

	require.NoErrorf(t, sm.Run(INSTANCE_COUNT, MAX_ROUNDS), "%s", sm.Describe())
	// Note: when async, the decision is not always the latest possible value,
	// but should be something recent.
	// This expectation may need to be relaxed.
	expected := sm.EC.Instances[INSTANCE_COUNT].Base
	expectInstanceDecision(t, sm, INSTANCE_COUNT-1, expected...)
}

func TestMultiSyncAgreement(t *testing.T) {
	t.Parallel()
	for n := 3; n <= 12; n++ {
		sm := sim.NewSimulation(SyncConfig(n), GraniteConfig(), sim.TraceNone)
		a := sm.Base(0).Extend(sm.CIDGen.Sample())
		// All nodes start with the same chain and will observe the same extensions of that chain
		// in subsequent instances.
		sm.SetChains(sim.ChainCount{Count: len(sm.Participants), Chain: a})
		require.NoErrorf(t, sm.Run(INSTANCE_COUNT, MAX_ROUNDS), "%s", sm.Describe())
		// Synchronous, agreeing groups always decide the candidate.
		expected := sm.EC.Instances[INSTANCE_COUNT].Base
		expectInstanceDecision(t, sm, INSTANCE_COUNT-1, expected...)
	}
}

func TestMultiAsyncAgreement(t *testing.T) {
	t.Parallel()
	for n := 3; n <= 12; n++ {
		sm := sim.NewSimulation(AsyncConfig(n, 0), GraniteConfig(), sim.TraceNone)
		sm.SetChains(sim.ChainCount{Count: n, Chain: sm.Base(0).Extend(sm.CIDGen.Sample())})

		require.NoErrorf(t, sm.Run(INSTANCE_COUNT, MAX_ROUNDS), "%s", sm.Describe())
		// Note: The expected decision only needs to be something recent.
		// Relax this expectation when the EC chain is less clean.
		expected := sm.EC.Instances[INSTANCE_COUNT].Base
		expectInstanceDecision(t, sm, INSTANCE_COUNT-1, expected...)
	}
}
