package f3

import (
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/assert"
)

type tipset struct {
	genesis time.Time
	period  time.Duration
	epoch   int64
}

func (ts *tipset) String() string {
	return fmt.Sprintf("epoch %d, timestamp %s", ts.epoch, ts.Timestamp())
}

func (ts *tipset) Key() gpbft.TipSetKey {
	panic("not implemented")
}

func (ts *tipset) Beacon() []byte {
	panic("not implemented")
}

func (ts *tipset) Epoch() int64 {
	return ts.epoch
}

func (ts *tipset) Timestamp() time.Time {
	return ts.genesis.Add(time.Duration(ts.epoch) * ts.period)
}

func tipsetGenerator(genesis time.Time, period time.Duration) func(epoch int64) *tipset {
	return func(epoch int64) *tipset {
		return &tipset{
			genesis: genesis,
			period:  period,
			epoch:   epoch,
		}
	}
}

func TestComputeTipsetTimestampAtEpoch(t *testing.T) {
	genesis := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	period := 30 * time.Second

	generateTipset := tipsetGenerator(genesis, period)
	tipset := generateTipset(10)

	t.Run("Basic Functionality", func(t *testing.T) {
		targetEpoch := int64(15)
		expected := generateTipset(targetEpoch).Timestamp()
		actual := computeTipsetTimestampAtEpoch(tipset, targetEpoch, period)
		assert.Equal(t, expected, actual)
	})

	t.Run("Zero Epoch", func(t *testing.T) {
		targetEpoch := int64(0)
		expected := generateTipset(targetEpoch).Timestamp()
		actual := computeTipsetTimestampAtEpoch(tipset, targetEpoch, period)
		assert.Equal(t, expected, actual)
	})

	t.Run("Large Epoch", func(t *testing.T) {
		largeEpoch := int64(1e6)
		expected := generateTipset(largeEpoch).Timestamp()
		actual := computeTipsetTimestampAtEpoch(tipset, largeEpoch, period)
		assert.Equal(t, expected, actual)
	})

	t.Run("Boundary Condition", func(t *testing.T) {
		boundaryEpoch := int64(1e3)
		expected := generateTipset(boundaryEpoch).Timestamp()
		actual := computeTipsetTimestampAtEpoch(tipset, boundaryEpoch, period)
		assert.Equal(t, expected, actual)
	})

	t.Run("Consistency", func(t *testing.T) {
		targetEpoch := int64(20)
		expected := generateTipset(targetEpoch).Timestamp()
		actual1 := computeTipsetTimestampAtEpoch(tipset, targetEpoch, period)
		actual2 := computeTipsetTimestampAtEpoch(tipset, targetEpoch, period)
		assert.Equal(t, expected, actual1)
		assert.Equal(t, expected, actual2)
	})
}
