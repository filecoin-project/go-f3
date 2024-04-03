package test

import (
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// repetitionParallelism sets the limit to the maximum degree of parallelism by which repetitions are executed.
// By default this value is set to runtime.NumCPU, and is overridable via F3_TEST_REPETITION_PARALLELISM environment variable.
// A negative value indicates no limit.
var repetitionParallelism int

func init() {
	repetitionParallelism = runtime.NumCPU()
	ps, found := os.LookupEnv("F3_TEST_REPETITION_PARALLELISM")
	if found {
		if v, err := strconv.ParseInt(ps, 10, 32); err == nil {
			repetitionParallelism = int(v)
		}
	}
}

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

// repeatInParallel repeats target for the given number of repetitions.
// See repetitionParallelism.
func repeatInParallel(t *testing.T, repetitions int, target func(t *testing.T, repetition int)) {
	var eg errgroup.Group
	eg.SetLimit(repetitionParallelism)
	for i := 1; i <= repetitions; i++ {
		repetition := i
		eg.Go(func() error {
			target(t, repetition)
			return nil
		})
	}
	require.NoError(t, eg.Wait())
}
