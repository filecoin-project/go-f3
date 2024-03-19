package test

import (
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
	"testing"
)

// Expects the decision in the first instance to be one of the given tipsets.
func expectDecision(t *testing.T, sm *sim.Simulation, expected ...gpbft.TipSet) {
	expectInstanceDecision(t, sm, 0, expected...)
}

// Expects the decision in an instance to be one of the given tipsets.
func expectInstanceDecision(t *testing.T, sm *sim.Simulation, instance uint64, expected ...gpbft.TipSet) {
nextParticipant:
	for _, participant := range sm.Participants {
		decision, ok := sm.GetDecision(instance, participant.ID())
		require.True(t, ok, "no decision for participant %d in instance %d", participant.ID(), instance)
		for _, e := range expected {
			if decision.Head() == e {
				continue nextParticipant
			}
		}
		require.Fail(t, "participant %d decided %s in instance %d, expected one of %s",
			participant.ID(), decision, instance, expected)
	}
}
