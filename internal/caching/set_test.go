package caching_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	subject := caching.NewSet(2)
	values, _ := generateValues(t, 4, 100)

	t.Run("does not contain unseen values", func(t *testing.T) {
		require.False(t, subject.ContainsOrAdd(values[0]))
		require.False(t, subject.ContainsOrAdd(values[1]))
	})
	t.Run("contains seen values", func(t *testing.T) {
		require.True(t, subject.Contains(values[0]))
		require.True(t, subject.Contains(values[1]))
	})
	t.Run("evicts first half once 2X capacity is reached", func(t *testing.T) {
		require.False(t, subject.ContainsOrAdd(values[2]))
		require.True(t, subject.Contains(values[0]))
		require.True(t, subject.Contains(values[1]))

		require.False(t, subject.ContainsOrAdd(values[3]))
		require.False(t, subject.ContainsOrAdd(values[0]))
		require.False(t, subject.ContainsOrAdd(values[1]))
	})
}

func TestSet_MinSizeIsOne(t *testing.T) {
	subject := caching.NewSet(-1)
	require.False(t, subject.ContainsOrAdd([]byte("a")))
	require.False(t, subject.ContainsOrAdd([]byte("b")))
	require.False(t, subject.ContainsOrAdd([]byte("c")))

	require.False(t, subject.ContainsOrAdd([]byte("a")))
	require.True(t, subject.ContainsOrAdd([]byte("a")))

	require.False(t, subject.ContainsOrAdd([]byte("b")))
	require.True(t, subject.ContainsOrAdd([]byte("b")))

	require.False(t, subject.ContainsOrAdd([]byte("c")))
	require.True(t, subject.ContainsOrAdd([]byte("c")))
}
