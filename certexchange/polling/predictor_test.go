package polling

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredictor(t *testing.T) {
	p := newPredictor(time.Second, 30*time.Second, 120*time.Second)

	// Progress of 1 shouldn't update anything.
	require.Equal(t, 30*time.Second, p.update(1))
	require.Equal(t, 30*time.Second, p.update(1))

	// Progress of 0 should predict the same interval, then twice that, etc.
	require.Equal(t, 30*time.Second, p.update(0))
	require.Equal(t, 60*time.Second, p.update(0))

	// After that, the intervalA should increase slightly.
	intervalA := p.update(1)
	require.Less(t, 30*time.Second, intervalA)
	require.Greater(t, 40*time.Second, intervalA)

	// If the interval is too large, it should decrease, but not by as much because we're
	// switching direction.
	intervalB := p.update(2)
	require.Less(t, 30*time.Second, intervalB)
	require.Greater(t, intervalA, intervalB)

	// It should keep getting smaller.
	intervalC := p.update(2)
	require.Greater(t, intervalB, intervalC)

	// Until we stabilize.
	require.Equal(t, intervalC, p.update(1))

	// We should always stay above the minimum.
	for i := 0; i < 100; i++ {
		require.LessOrEqual(t, time.Second, p.update(2))
	}
	require.Equal(t, time.Second, p.update(1))

	// And below the maximum (unless backing off).
	for i := 0; i < 100; i++ {
		require.GreaterOrEqual(t, 120*time.Second, p.update(0))
		require.GreaterOrEqual(t, 120*time.Second, p.update(1))
	}
	require.Equal(t, 120*time.Second, p.update(1))

	// But backoff should go much higher.
	for i := 0; i < 100; i++ {
		require.GreaterOrEqual(t, 10*120*time.Second, p.update(0))
	}
	require.Equal(t, 10*120*time.Second, p.update(0))

	// And revert to the old time when done.
	require.Equal(t, 120*time.Second, p.update(1))
}

func TestPredictorConverges(t *testing.T) {
	const minSeconds = 1
	const maxSeconds = 120
	p := newPredictor(minSeconds*time.Second, 30*time.Second, maxSeconds*time.Second)

	converge := func(interval time.Duration, n int) (time.Duration, time.Duration, int) {
		currentTime := time.Duration(0)
		updatesSeen := uint64(0)
		for i := 0; i < n; i++ {
			newUpdatesSeen := uint64(currentTime / interval)
			currentTime += p.update(newUpdatesSeen - updatesSeen)
			updatesSeen = newUpdatesSeen
		}
		return p.update(1), currentTime, int(currentTime / interval)
	}

	// Converges from 30s -> 5s very quickly.
	{
		res, ellapsed, count := converge(5*time.Second, 10)
		assert.InDelta(t, 5*time.Second, res, float64(1*time.Second))
		assert.Less(t, ellapsed, 3*time.Minute)
		assert.Less(t, count, 30)
	}

	r := rand.New(rand.NewSource(0xdeadbeef))
	numbers := r.Perm(maxSeconds - minSeconds)
	for _, n := range numbers {
		eventInterval := time.Duration(n+minSeconds) * time.Second
		result, _, _ := converge(eventInterval, 300)
		assert.InEpsilon(t, eventInterval, result, 0.05, "actual %s, expected %s", result, eventInterval)
	}
}
