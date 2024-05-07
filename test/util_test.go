package test

import (
	"bytes"
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
func requireConsensus(t *testing.T, sm *sim.Simulation, expected ...gpbft.TipSet) {
	requireConsensusAtInstance(t, sm, 0, expected...)
}

// Expects the decision in an instance to be one of the given tipsets.
func requireConsensusAtInstance(t *testing.T, sm *sim.Simulation, instance uint64, expected ...gpbft.TipSet) {
nextParticipant:
	for _, pid := range sm.ListParticipantIDs() {
		decision, ok := sm.GetDecision(instance, pid)
		require.True(t, ok, "no decision for participant %d in instance %d", pid, instance)
		for _, e := range expected {
			if bytes.Equal(decision.Head(), e) {
				continue nextParticipant
			}
		}
		require.Fail(t, "consensus not reached", "participant %d decided %s in instance %d, expected one of %s",
			pid, decision, instance, expected)
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
