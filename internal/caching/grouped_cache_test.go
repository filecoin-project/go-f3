package caching_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func TestGroupedSet(t *testing.T) {

	subject := caching.NewGroupedSet(3, 2)

	g1v1 := func() (uint64, []byte, []byte) { return 1, []byte("undadasea"), []byte("fish") }
	g1v2 := func() (uint64, []byte, []byte) { return 1, []byte("undadasea"), []byte("lobster") }
	g1v3 := func() (uint64, []byte, []byte) { return 1, []byte("undadasea"), []byte("lobstermuncher") }
	g1v4 := func() (uint64, []byte, []byte) { return 1, []byte("undadasea"), []byte("barreleye") }
	g2v1 := func() (uint64, []byte, []byte) { return 2, []byte("undadasea"), []byte("fish") }
	g3v1 := func() (uint64, []byte, []byte) { return 3, []byte("undadasea"), []byte("fish") }
	g4v1 := func() (uint64, []byte, []byte) { return 4, []byte("undadasea"), []byte("fish") }

	t.Run("does not contain unseen values", func(t *testing.T) {
		contains, err := subject.Contains(g1v1())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v2())
		require.NoError(t, err)
		require.False(t, contains)
	})
	t.Run("contains seen values", func(t *testing.T) {
		added, err := subject.Add(g1v1())
		require.NoError(t, err)
		require.True(t, added)

		contains, err := subject.Contains(g1v1())
		require.NoError(t, err)
		require.True(t, contains)

		added, err = subject.Add(g1v2())
		require.NoError(t, err)
		require.True(t, added)

		contains, err = subject.Contains(g1v2())
		require.NoError(t, err)
		require.True(t, contains)
	})
	t.Run("evicts least recent group", func(t *testing.T) {

		// Add 3 distinct groups to cause an eviction
		added, err := subject.Add(g2v1())
		require.NoError(t, err)
		require.True(t, added)

		added, err = subject.Add(g3v1())
		require.NoError(t, err)
		require.True(t, added)

		added, err = subject.Add(g4v1())
		require.NoError(t, err)
		require.True(t, added)

		// Assert group1 is evicted
		contains, err := subject.Contains(g1v1())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v2())
		require.NoError(t, err)
		require.False(t, contains)
	})

	t.Run("evicts first half of set on 2X max", func(t *testing.T) {
		// Assert group1 has 2 entries.
		added, err := subject.Add(g1v1())
		require.NoError(t, err)
		require.True(t, added)

		added, err = subject.Add(g1v2())
		require.NoError(t, err)
		require.True(t, added)

		// Add enough values to group 1 to cause eviction
		added, err = subject.Add(g1v3())
		require.NoError(t, err)
		require.True(t, added)

		added, err = subject.Add(g1v4())
		require.NoError(t, err)
		require.True(t, added)

		// assert first half of entries in group 1 are evicted
		contains, err := subject.Contains(g1v1())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v2())
		require.NoError(t, err)
		require.False(t, contains)
	})

	t.Run("explicit group removal is removed", func(t *testing.T) {
		// Assert group 1 exists and removed
		require.True(t, subject.RemoveGroupsLessThan(2))

		// Assert group 1 is already removed
		require.False(t, subject.RemoveGroupsLessThan(2))

		contains, err := subject.Contains(g1v1())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v2())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v3())
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = subject.Contains(g1v4())
		require.NoError(t, err)
		require.False(t, contains)

	})
}
