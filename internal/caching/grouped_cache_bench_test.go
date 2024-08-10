package caching_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func BenchmarkGroupedSet(b *testing.B) {
	benchmarkGroupedSetBySize(b, 1, 1000, 128)
	benchmarkGroupedSetBySize(b, 10, 1000, 128)
	benchmarkGroupedSetBySize(b, 100, 1000, 128)
}

func benchmarkGroupedSetBySize(b *testing.B, maxGroups, maxSetSize, valueLen int) {
	b.Run(fmt.Sprintf("%d/%d/%d/Add", maxGroups, maxSetSize, valueLen), func(b *testing.B) {
		groupedValues, size := generateGroupedValues(b, maxGroups, maxSetSize*2, valueLen)
		b.SetBytes(size)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				subject := caching.NewGroupedSet(maxGroups, maxSetSize)
				for group, values := range groupedValues {
					for _, value := range values {
						added, err := subject.Add(uint64(group), nil, value)
						require.NoError(b, err)
						require.True(b, added)
					}
				}
			}
		})
	})

	b.Run(fmt.Sprintf("%d/%d/%d/Contains_Existing", maxGroups, maxSetSize, valueLen), func(b *testing.B) {
		subject := caching.NewGroupedSet(maxGroups, maxSetSize)
		maxElementsBeforeEviction := (maxSetSize * 2) - 1
		groupedValues, size := generateGroupedValues(b, maxGroups, maxElementsBeforeEviction, valueLen)
		for group, values := range groupedValues {
			for _, value := range values {
				added, err := subject.Add(uint64(group), nil, value)
				require.NoError(b, err)
				require.True(b, added)
			}
		}

		b.SetBytes(size)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for group, values := range groupedValues {
					for _, value := range values {
						contains, err := subject.Contains(uint64(group), nil, value)
						require.NoError(b, err)
						require.True(b, contains)
					}
				}
			}
		})
	})
}

func generateGroupedValues(b testing.TB, groups, count, len int) ([][][]byte, int64) {
	values := make([][][]byte, count)
	var totalSize int64
	for i := 0; i < groups; i++ {
		value, size := generateValues(b, count, len)
		values[i] = value
		totalSize += size
	}
	rand.Shuffle(groups, func(one, other int) {
		values[one], values[other] = values[other], values[one]
	})
	return values, totalSize
}
