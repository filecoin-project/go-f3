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
		contains, err := subject.ContainsOrAdd(nil, values[0])
		require.NoError(t, err)
		require.False(t, contains)
		contains, err = subject.ContainsOrAdd(nil, values[1])
		require.NoError(t, err)
		require.False(t, contains)
	})
	t.Run("contains seen values", func(t *testing.T) {
		contains, err := subject.Contains(nil, values[0])
		require.NoError(t, err)
		require.True(t, contains)
		contains, err = subject.Contains(nil, values[1])
		require.NoError(t, err)
		require.True(t, contains)
	})
	t.Run("evicts first half once 2X capacity is reached", func(t *testing.T) {
		contains, err := subject.ContainsOrAdd(nil, values[2])
		require.NoError(t, err)
		require.False(t, contains)
		contains, err = subject.Contains(nil, values[0])
		require.NoError(t, err)
		require.True(t, contains)
		contains, err = subject.Contains(nil, values[1])
		require.NoError(t, err)
		require.True(t, contains)

		contains, err = subject.ContainsOrAdd(nil, values[3])
		require.NoError(t, err)
		require.False(t, contains)
		contains, err = subject.ContainsOrAdd(nil, values[0])
		require.NoError(t, err)
		require.False(t, contains)
		contains, err = subject.ContainsOrAdd(nil, values[1])
		require.NoError(t, err)
		require.False(t, contains)
	})
}

func TestSet_MinSizeIsOne(t *testing.T) {
	subject := caching.NewSet(-1)
	contains, err := subject.ContainsOrAdd(nil, []byte("a"))
	require.NoError(t, err)
	require.False(t, contains)
	contains, err = subject.ContainsOrAdd(nil, []byte("b"))
	require.NoError(t, err)
	require.False(t, contains)
	contains, err = subject.ContainsOrAdd(nil, []byte("c"))
	require.NoError(t, err)
	require.False(t, contains)

	contains, err = subject.ContainsOrAdd(nil, []byte("a"))
	require.NoError(t, err)
	require.False(t, contains)
	contains, err = subject.ContainsOrAdd(nil, []byte("a"))
	require.NoError(t, err)
	require.True(t, contains)

	contains, err = subject.ContainsOrAdd(nil, []byte("b"))
	require.NoError(t, err)
	require.False(t, contains)
	contains, err = subject.ContainsOrAdd(nil, []byte("b"))
	require.NoError(t, err)
	require.True(t, contains)

	contains, err = subject.ContainsOrAdd(nil, []byte("c"))
	require.NoError(t, err)
	require.False(t, contains)
	contains, err = subject.ContainsOrAdd(nil, []byte("c"))
	require.NoError(t, err)
	require.True(t, contains)
}
