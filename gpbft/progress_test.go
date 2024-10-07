package gpbft

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtomicProgression(t *testing.T) {
	subject := newAtomicProgression()
	t.Run("zero value", func(t *testing.T) {
		instant := subject.Get()
		require.Equal(t, uint64(0), instant.ID, "Expected initial instance to be 0")
		require.Equal(t, uint64(0), instant.Round, "Expected initial round to be 0")
		require.Equal(t, INITIAL_PHASE, instant.Phase, "Expected initial phase to be INITIAL_PHASE")
	})
	t.Run("notify and get", func(t *testing.T) {
		subject.NotifyProgress(Instant{1, 10, PREPARE_PHASE})
		instant := subject.Get()
		require.Equal(t, uint64(1), instant.ID, "Expected instance to be 1")
		require.Equal(t, uint64(10), instant.Round, "Expected round to be 10")
		require.Equal(t, PREPARE_PHASE, instant.Phase, "Expected phase to be PREPARE_PHASE")
	})
	t.Run("notify and get progresses", func(t *testing.T) {
		subject.NotifyProgress(Instant{2, 20, COMMIT_PHASE})
		instant := subject.Get()
		require.Equal(t, uint64(2), instant.ID, "Expected instance to be updated to 2")
		require.Equal(t, uint64(20), instant.Round, "Expected round to be updated to 20")
		require.Equal(t, COMMIT_PHASE, instant.Phase, "Expected phase to be updated to COMMIT_PHASE")
	})
	t.Run("concurrent update", func(t *testing.T) {
		var wg sync.WaitGroup
		update := func(inst, rnd uint64, ph Phase) {
			defer wg.Done()
			subject.NotifyProgress(Instant{inst, rnd, ph})
		}
		wg.Add(2)
		go update(3, 30, COMMIT_PHASE)
		go update(4, 40, DECIDE_PHASE)
		wg.Wait()

		instant := subject.Get()
		require.True(t, instant.ID == 3 || instant.ID == 4, "Instance should match one of the updates")
		require.True(t, instant.Round == 30 || instant.Round == 40, "Round should match one of the updates")
		require.True(t, instant.Phase == COMMIT_PHASE || instant.Phase == DECIDE_PHASE, "Phase should match one of the updates")
	})
}
