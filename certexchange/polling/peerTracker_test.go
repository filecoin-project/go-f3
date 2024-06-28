package polling

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerRecordHitMiss(t *testing.T) {
	r := new(peerRecord)

	{
		hitRate, count := r.hitRate()
		// should initialize to some "assumed" positive value.
		require.Greater(t, hitRate, 0.0)
		require.Equal(t, 0, count)
	}

	// 3 hits
	for i := 1; i <= 3; i++ {
		r.recordHit()
		hitRate, count := r.hitRate()
		require.Equal(t, 1.0, hitRate)
		require.Equal(t, i, count)
	}

	// 3 misses
	for i := 4; i <= 6; i++ {
		r.recordMiss()
		hitRate, count := r.hitRate()
		require.Less(t, hitRate, 1.0)
		require.Equal(t, i, count)
	}

	// Should be 50/50.
	{
		hitRate, count := r.hitRate()
		require.Equal(t, 6, count)
		require.Equal(t, 0.5, hitRate)
	}

	// 10 hits
	for i := 0; i < 7; i++ {
		r.recordHit()
	}

	// Still shouldn't be 100% hit, but should be "full"
	{
		hitRate, count := r.hitRate()
		require.Less(t, hitRate, 1.0)
		require.Equal(t, 10, count)
	}

	// 3 more hits
	for i := 0; i <= 3; i++ {
		r.recordHit()
	}

	// Should now be 100% hit (canceling out the misses).
	{
		hitRate, count := r.hitRate()
		require.Equal(t, hitRate, 1.0)
		require.Equal(t, 10, count)
	}

	// 10 misses should bring us back to 50/50

	for i := 0; i < 10; i++ {
		r.recordMiss()
	}

	{
		hitRate, _ := r.hitRate()
		require.Equal(t, hitRate, 0.5)
	}

	// Another 10 should bring us to 0.0

	for i := 0; i < 10; i++ {
		r.recordMiss()
	}

	{
		hitRate, _ := r.hitRate()
		require.Equal(t, hitRate, 0.0)
	}
}
