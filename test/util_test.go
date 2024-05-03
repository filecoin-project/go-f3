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
func requireConsensusAtFirstInstance(t *testing.T, sm *sim.Simulation, expectAnyOf ...gpbft.TipSet) {
	requireConsensusAtInstance(t, sm, 0, expectAnyOf...)
}

// Expects the decision in an instance to be one of the given tipsets.
func requireConsensusAtInstance(t *testing.T, sm *sim.Simulation, instance uint64, expectAnyOf ...gpbft.TipSet) {
	inst := sm.GetInstance(instance)
	for _, pid := range sm.ListParticipantIDs() {
		require.NotNil(t, inst, "no such instance")
		decision := inst.GetDecision(pid)
		require.NotNil(t, decision, "no decision for participant %d in instance %d", pid, instance)
		require.Contains(t, expectAnyOf, decision.Head(), "consensus not reached: participant %d decided %s in instance %d, expected any of %s",
			pid, decision.Head(), instance, expectAnyOf)
	}
}

func generateECChain(t *testing.T, tsg *sim.TipSetGenerator) gpbft.ECChain {
	t.Helper()
	// TODO: add stochastic chain generation.
	chain, err := gpbft.NewChain(tsg.Sample())
	require.NoError(t, err)
	return chain
}

// repeatInParallel repeats target for the given number of repetitions.
// Set F3_TEST_REPETITION_PARALLELISM=1 to run repetitions sequentially.
// See repetitionParallelism.
func repeatInParallel(t *testing.T, repetitions int, target func(t *testing.T, repetition int)) {
	// When no parallelism is requested, run repetitions sequentially so their logs are readable.
	if repetitionParallelism <= 1 {
		for i := 0; i <= repetitions; i++ {
			t.Log("repetition", i)
			target(t, i)
		}
	} else {
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
}
