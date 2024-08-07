package f3

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleSet(t *testing.T) {
	subject := newSampleSet(2)

	v1 := []byte("fish")
	v2 := []byte("lobster")
	v3 := []byte("barreleye")
	v4 := []byte("lobstermuncher")

	t.Run("does not contain unseen values", func(t *testing.T) {
		require.False(t, subject.contains(v1))
		require.False(t, subject.contains(v2))
	})
	t.Run("contains seen values", func(t *testing.T) {
		require.True(t, subject.contains(v1))
		require.True(t, subject.contains(v2))
	})
	t.Run("evicts first half once 2X capacity is reached", func(t *testing.T) {
		require.False(t, subject.contains(v3))
		require.True(t, subject.contains(v1))
		require.True(t, subject.contains(v2))

		require.False(t, subject.contains(v4))
		require.False(t, subject.contains(v1))
		require.False(t, subject.contains(v2))
	})
	t.Run("limits keys to 96 bytes", func(t *testing.T) {
		longKey := make([]byte, 100)
		n, err := rand.Read(longKey)
		require.NoError(t, err)
		require.Equal(t, 100, n)
		require.False(t, subject.contains(longKey))
		require.True(t, subject.contains(longKey))
		require.True(t, subject.contains(longKey[:96]))
	})
}

func TestSampleSet_MinSizeIsOne(t *testing.T) {
	subject := newSampleSet(-1)
	require.False(t, subject.contains([]byte("a")))
	require.False(t, subject.contains([]byte("b")))
	require.False(t, subject.contains([]byte("c")))

	require.False(t, subject.contains([]byte("a")))
	require.True(t, subject.contains([]byte("a")))

	require.False(t, subject.contains([]byte("b")))
	require.True(t, subject.contains([]byte("b")))

	require.False(t, subject.contains([]byte("c")))
	require.True(t, subject.contains([]byte("c")))
}
