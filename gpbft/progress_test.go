package gpbft

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtomicProgression(t *testing.T) {
	subject := newAtomicProgression()
	t.Run("zero value", func(t *testing.T) {
		instance, round, phase := subject.Get()
		require.Equal(t, uint64(0), instance, "Expected initial instance to be 0")
		require.Equal(t, uint64(0), round, "Expected initial round to be 0")
		require.Equal(t, INITIAL_PHASE, phase, "Expected initial phase to be INITIAL_PHASE")
	})
	t.Run("notify and get", func(t *testing.T) {
		subject.NotifyProgress(1, 10, PREPARE_PHASE)
		instance, round, phase := subject.Get()
		require.Equal(t, uint64(1), instance, "Expected instance to be 1")
		require.Equal(t, uint64(10), round, "Expected round to be 10")
		require.Equal(t, PREPARE_PHASE, phase, "Expected phase to be PREPARE_PHASE")
	})
	t.Run("notify and get progresses", func(t *testing.T) {
		subject.NotifyProgress(2, 20, COMMIT_PHASE)
		instance, round, phase := subject.Get()
		require.Equal(t, uint64(2), instance, "Expected instance to be updated to 2")
		require.Equal(t, uint64(20), round, "Expected round to be updated to 20")
		require.Equal(t, COMMIT_PHASE, phase, "Expected phase to be updated to COMMIT_PHASE")
	})
	t.Run("concurrent update", func(t *testing.T) {
		var wg sync.WaitGroup
		update := func(inst, rnd uint64, ph Phase) {
			defer wg.Done()
			subject.NotifyProgress(inst, rnd, ph)
		}
		wg.Add(2)
		go update(3, 30, COMMIT_PHASE)
		go update(4, 40, DECIDE_PHASE)
		wg.Wait()

		instance, round, phase := subject.Get()
		require.True(t, instance == 3 || instance == 4, "Instance should match one of the updates")
		require.True(t, round == 30 || round == 40, "Round should match one of the updates")
		require.True(t, phase == COMMIT_PHASE || phase == DECIDE_PHASE, "Phase should match one of the updates")
	})
}
