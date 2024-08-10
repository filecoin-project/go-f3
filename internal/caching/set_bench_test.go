package caching_test

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func BenchmarkSet(b *testing.B) {
	benchmarkSetBySize(b, 1, 128)
	benchmarkSetBySize(b, 100, 128)
	benchmarkSetBySize(b, 10_000, 128)
}

func benchmarkSetBySize(b *testing.B, maxSetSize, valueLen int) {
	b.Run(fmt.Sprintf("%d/%d/ContainsOrAdd_New", maxSetSize, valueLen), func(b *testing.B) {
		values, size := generateValues(b, maxSetSize*2, valueLen)
		b.SetBytes(size)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				subject := caching.NewSet(maxSetSize)
				for _, value := range values {
					contained, err := subject.ContainsOrAdd(nil, value)
					require.NoError(b, err)
					require.False(b, contained)
				}
			}
		})
	})

	b.Run(fmt.Sprintf("%d/%d/ContainsOrAdd_Existing", maxSetSize, valueLen), func(b *testing.B) {
		subject := caching.NewSet(maxSetSize)
		maxElementsBeforeEviction := (maxSetSize * 2) - 1
		values, size := generateValues(b, maxElementsBeforeEviction, valueLen)
		for _, value := range values {
			contained, err := subject.ContainsOrAdd(nil, value)
			require.NoError(b, err)
			require.False(b, contained)
		}

		b.SetBytes(size)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for _, value := range values {
					contained, err := subject.ContainsOrAdd(nil, value)
					require.NoError(b, err)
					require.True(b, contained)
				}
			}
		})
	})
}

func generateValues(b testing.TB, count, len int) ([][]byte, int64) {
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		value := make([]byte, len)
		n, err := rand.Read(value)
		require.NoError(b, err)
		require.Equal(b, n, len)
		values[i] = value
	}
	mrand.Shuffle(count, func(one, other int) {
		values[one], values[other] = values[other], values[one]
	})
	return values, int64(count * len)
}
