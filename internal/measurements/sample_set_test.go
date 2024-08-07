package measurements_test

import (
	"crypto/rand"
	"testing"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"github.com/stretchr/testify/require"
)

func TestSampleSet(t *testing.T) {
	subject := measurements.NewSampleSet(2)

	v1 := []byte("fish")
	v2 := []byte("lobster")
	v3 := []byte("barreleye")
	v4 := []byte("lobstermuncher")

	t.Run("does not contain unseen values", func(t *testing.T) {
		require.False(t, subject.Contains(v1))
		require.False(t, subject.Contains(v2))
	})
	t.Run("contains seen values", func(t *testing.T) {
		require.True(t, subject.Contains(v1))
		require.True(t, subject.Contains(v2))
	})
	t.Run("evicts first half once 2X capacity is reached", func(t *testing.T) {
		require.False(t, subject.Contains(v3))
		require.True(t, subject.Contains(v1))
		require.True(t, subject.Contains(v2))

		require.False(t, subject.Contains(v4))
		require.False(t, subject.Contains(v1))
		require.False(t, subject.Contains(v2))
	})
	t.Run("limits keys to 96 bytes", func(t *testing.T) {
		longKey := make([]byte, 100)
		n, err := rand.Read(longKey)
		require.NoError(t, err)
		require.Equal(t, 100, n)
		require.False(t, subject.Contains(longKey))
		require.True(t, subject.Contains(longKey))
		require.True(t, subject.Contains(longKey[:96]))
	})
}

func TestSampleSet_MinSizeIsOne(t *testing.T) {
	subject := measurements.NewSampleSet(-1)
	require.False(t, subject.Contains([]byte("a")))
	require.False(t, subject.Contains([]byte("b")))
	require.False(t, subject.Contains([]byte("c")))

	require.False(t, subject.Contains([]byte("a")))
	require.True(t, subject.Contains([]byte("a")))

	require.False(t, subject.Contains([]byte("b")))
	require.True(t, subject.Contains([]byte("b")))

	require.False(t, subject.Contains([]byte("c")))
	require.True(t, subject.Contains([]byte("c")))
}
