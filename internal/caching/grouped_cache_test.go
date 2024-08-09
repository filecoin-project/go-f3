package caching_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func TestGroupedSet(t *testing.T) {

	subject := caching.NewGroupedSet(3, 2)

	g1v1 := func() (uint64, []byte) { return 1, []byte("fish") }
	g1v2 := func() (uint64, []byte) { return 1, []byte("lobster") }
	g1v3 := func() (uint64, []byte) { return 1, []byte("lobstermuncher") }
	g1v4 := func() (uint64, []byte) { return 1, []byte("barreleye") }
	g2v1 := func() (uint64, []byte) { return 2, []byte("fish") }
	g3v1 := func() (uint64, []byte) { return 3, []byte("fish") }
	g4v1 := func() (uint64, []byte) { return 4, []byte("fish") }

	t.Run("does not contain unseen values", func(t *testing.T) {
		require.False(t, subject.Contains(g1v1()))
		require.False(t, subject.Contains(g1v2()))
	})
	t.Run("contains seen values", func(t *testing.T) {
		require.True(t, subject.Add(g1v1()))
		require.True(t, subject.Contains(g1v1()))
		require.True(t, subject.Add(g1v2()))
		require.True(t, subject.Contains(g1v2()))
	})
	t.Run("evicts least recent group", func(t *testing.T) {

		// Add 3 distinct groups to cause an eviction
		require.True(t, subject.Add(g2v1()))
		require.True(t, subject.Add(g3v1()))
		require.True(t, subject.Add(g4v1()))

		// Assert group1 is evicted
		require.False(t, subject.Contains(g1v1()))
		require.False(t, subject.Contains(g1v2()))
	})

	t.Run("evicts first half of set on 2X max", func(t *testing.T) {
		// Assert group1 has 2 entries.
		require.True(t, subject.Add(g1v1()))
		require.True(t, subject.Add(g1v2()))

		// Add enough values to group 1 to cause eviction
		require.True(t, subject.Add(g1v3()))
		require.True(t, subject.Add(g1v4()))

		// assert first half of entries in group 1 are evicted
		require.False(t, subject.Contains(g1v1()))
		require.False(t, subject.Contains(g1v2()))
	})

	t.Run("explicit group removal is removed", func(t *testing.T) {
		// Assert group 1 exists and removed
		require.True(t, subject.RemoveGroup(1))

		// Assert group 1 is already removed
		require.False(t, subject.RemoveGroup(1))
		require.False(t, subject.Contains(g1v1()))
		require.False(t, subject.Contains(g1v2()))
		require.False(t, subject.Contains(g1v3()))
		require.False(t, subject.Contains(g1v4()))
	})
}
