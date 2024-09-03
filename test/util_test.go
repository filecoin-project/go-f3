package test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
)

// Expects the decision in the first instance to be one of the given tipsets.
func requireConsensusAtFirstInstance(t *testing.T, sm *sim.Simulation, expectAnyOf ...gpbft.TipSet) {
	t.Helper()
	requireConsensusAtInstance(t, sm, 0, expectAnyOf...)
}

// Expects the decision in an instance to be one of the given tipsets.
func requireConsensusAtInstance(t *testing.T, sm *sim.Simulation, instance uint64, expectAnyOf ...gpbft.TipSet) {
	t.Helper()
	inst := sm.GetInstance(instance)
	for _, pid := range sm.ListParticipantIDs() {
		require.NotNil(t, inst, "no such instance")
		decision := inst.GetDecision(pid)
		require.NotNil(t, decision, "no decision for participant %d in instance %d", pid, instance)
		require.Contains(t, expectAnyOf, *decision.Head(), "consensus not reached: participant %d decided %s in instance %d, expected any of %s",
			pid, decision.Head(), instance, gpbft.ECChain(expectAnyOf))
	}
}

func generateECChain(t *testing.T, tsg *sim.TipSetGenerator) gpbft.ECChain {
	t.Helper()
	// TODO: add stochastic chain generation.
	chain, err := gpbft.NewChain(gpbft.TipSet{
		Epoch:      0,
		Key:        tsg.Sample(),
		PowerTable: gpbft.MakeCid([]byte("pt")),
	})
	require.NoError(t, err)
	return chain
}
