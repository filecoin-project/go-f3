package gpbft_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestECChain(t *testing.T) {
	t.Parallel()

	zeroTipSet := gpbft.TipSet{}
	oneTipSet := gpbft.TipSet{Epoch: 0, Key: []byte{1}, PowerTable: []byte("pt")}
	t.Run("zero-value is zero", func(t *testing.T) {
		var subject gpbft.ECChain
		require.True(t, subject.IsZero())
		require.False(t, subject.HasBase(&zeroTipSet))
		require.False(t, subject.HasPrefix(subject))
		require.False(t, subject.HasTipset(&zeroTipSet))
		require.False(t, subject.SameBase(subject))
		require.True(t, subject.Eq(subject))
		require.True(t, subject.Eq(*new(gpbft.ECChain)))
		require.Nil(t, subject.Suffix())
		require.Panics(t, func() { subject.Prefix(0) })
		require.Panics(t, func() { subject.Base() })
		require.Panics(t, func() { subject.Head() })
		require.NoError(t, subject.Validate())
	})
	t.Run("NewChain with zero-value base is error", func(t *testing.T) {
		subject, err := gpbft.NewChain(zeroTipSet)
		require.Error(t, err)
		require.Nil(t, subject)
	})
	t.Run("extended chain is as expected", func(t *testing.T) {
		wantBase := gpbft.TipSet{Epoch: 0, Key: []byte("fish"), PowerTable: []byte("pt")}
		subject, err := gpbft.NewChain(wantBase)
		require.NoError(t, err)
		require.Len(t, subject, 1)
		require.Equal(t, &wantBase, subject.Base())
		require.Equal(t, &wantBase, subject.Head())
		require.False(t, subject.HasSuffix())
		require.NoError(t, subject.Validate())

		wantNext := gpbft.TipSet{Epoch: 1, Key: []byte("lobster"), PowerTable: []byte("pt")}
		subjectExtended := subject.Extend(wantNext.Key)
		require.Len(t, subjectExtended, 2)
		require.NoError(t, subjectExtended.Validate())
		require.Equal(t, &wantBase, subjectExtended.Base())
		require.Equal(t, []gpbft.TipSet{wantNext}, subjectExtended.Suffix())
		require.Equal(t, &wantNext, subjectExtended.Head())
		require.True(t, subjectExtended.HasSuffix())
		require.Equal(t, &wantNext, subjectExtended.Prefix(1).Head())
		require.True(t, subjectExtended.HasTipset(&wantBase))
		require.False(t, subject.HasPrefix(subjectExtended))
		require.True(t, subjectExtended.HasPrefix(subject))

		require.False(t, subject.Extend(wantBase.Key).HasPrefix(subjectExtended.Extend(wantNext.Key)))
	})
	t.Run("SameBase is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain(oneTipSet)
		require.NoError(t, err)
		require.False(t, nonZeroChain.SameBase(zeroChain))
		require.False(t, zeroChain.SameBase(nonZeroChain))
		require.False(t, zeroChain.SameBase(zeroChain))
	})
	t.Run("HasPrefix is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain(oneTipSet)
		require.NoError(t, err)
		require.False(t, nonZeroChain.HasPrefix(zeroChain))
		require.False(t, zeroChain.HasPrefix(nonZeroChain))
		require.False(t, zeroChain.HasPrefix(zeroChain))
	})
	t.Run("zero-valued chain is valid", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		require.NoError(t, zeroChain.Validate())
	})
	t.Run("ordered chain with zero-valued base is invalid", func(t *testing.T) {
		subject := gpbft.ECChain{zeroTipSet, oneTipSet}
		require.Error(t, subject.Validate())
	})

	t.Run("prefix and extend don't mutate", func(t *testing.T) {
		subject := gpbft.ECChain{
			gpbft.TipSet{Epoch: 0, Key: []byte{0}, PowerTable: []byte("pt")},
			gpbft.TipSet{Epoch: 1, Key: []byte{1}, PowerTable: []byte("pt1")},
		}
		dup := append(gpbft.ECChain{}, subject...)
		after := subject.Prefix(0).Extend([]byte{2})
		require.True(t, subject.Eq(dup))
		require.True(t, after.Eq(gpbft.ECChain{
			gpbft.TipSet{Epoch: 0, Key: []byte{0}, PowerTable: []byte("pt")},
			gpbft.TipSet{Epoch: 1, Key: []byte{2}, PowerTable: []byte("p2")},
		}))
	})

	t.Run("extending multiple times doesn't clobber", func(t *testing.T) {
		// simulate over-allocation
		initial := gpbft.ECChain{gpbft.TipSet{Epoch: 0, Key: []byte{0}}, gpbft.TipSet{}}[:1]

		first := initial.Extend([]byte{1})
		second := initial.Extend([]byte{2})
		require.Equal(t, first[1], gpbft.TipSet{Epoch: 1, Key: []byte{1}})
		require.Equal(t, second[1], gpbft.TipSet{Epoch: 1, Key: []byte{2}})
	})
}
