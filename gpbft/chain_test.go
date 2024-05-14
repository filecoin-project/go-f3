package gpbft_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestTipSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		subject    gpbft.TipSet
		wantZero   bool
		wantString string
	}{
		{
			name:       "zero-value struct is zero",
			wantZero:   true,
			wantString: "",
		},
		{
			name:       "ZeroTipSet is zero",
			subject:    []byte{},
			wantZero:   true,
			wantString: "",
		},
		{
			name:       "NewTipSet with zero values is zero",
			subject:    nil,
			wantZero:   true,
			wantString: "",
		},
		{
			name:       "Non-zero is not zero",
			subject:    gpbft.TipSet("fish"),
			wantString: "fish",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantZero, len(test.subject) == 0)
			require.Equal(t, test.wantString, string(test.subject))
		})
	}
}

func TestECChain(t *testing.T) {
	t.Parallel()

	zeroTipSet := []byte{}
	t.Run("zero-value is zero", func(t *testing.T) {
		var subject gpbft.ECChain
		require.True(t, subject.IsZero())
		require.False(t, subject.HasBase(zeroTipSet))
		require.False(t, subject.HasPrefix(subject))
		require.False(t, subject.HasTipset(zeroTipSet))
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
		wantBase := []byte("fish")
		subject, err := gpbft.NewChain(wantBase)
		require.NoError(t, err)
		require.Len(t, subject, 1)
		require.Equal(t, wantBase, subject.Base())
		require.Equal(t, wantBase, subject.Head())
		require.NoError(t, subject.Validate())

		wantNext := []byte("lobster")
		subjectExtended := subject.Extend(wantNext)
		require.Len(t, subjectExtended, 2)
		require.NoError(t, subjectExtended.Validate())
		require.Equal(t, wantBase, subjectExtended.Base())
		require.Equal(t, []gpbft.TipSet{wantNext}, subjectExtended.Suffix())
		require.Equal(t, wantNext, subjectExtended.Head())
		require.Equal(t, wantNext, subjectExtended.Prefix(1).Head())
		require.True(t, subjectExtended.HasTipset(wantBase))
		require.False(t, subject.HasPrefix(subjectExtended))
		require.True(t, subjectExtended.HasPrefix(subject))

		require.False(t, subject.Extend(wantBase).HasPrefix(subjectExtended.Extend(wantNext)))
	})
	t.Run("SameBase is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain([]byte{1})
		require.NoError(t, err)
		require.False(t, nonZeroChain.SameBase(zeroChain))
		require.False(t, zeroChain.SameBase(nonZeroChain))
		require.False(t, zeroChain.SameBase(zeroChain))
	})
	t.Run("HasPrefix is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain([]byte{1})
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
		subject := gpbft.ECChain{zeroTipSet, []byte{1}}
		require.Error(t, subject.Validate())
	})

	t.Run("prefix and extend don't mutate", func(t *testing.T) {
		subject := gpbft.ECChain{[]byte{0}, []byte{1}}
		dup := append(gpbft.ECChain{}, subject...)
		after := subject.Prefix(0).Extend([]byte{2})
		require.True(t, subject.Eq(dup))
		require.True(t, after.Eq(gpbft.ECChain{[]byte{0}, []byte{2}}))
	})

	t.Run("extending multiple times doesn't clobber", func(t *testing.T) {
		// simulate over-allocation
		initial := gpbft.ECChain{[]byte{0}, []byte{0}}[:1]

		first := initial.Extend([]byte{1})
		second := initial.Extend([]byte{2})
		require.Equal(t, first[1], []byte{1})
		require.Equal(t, second[1], []byte{2})
	})
}
