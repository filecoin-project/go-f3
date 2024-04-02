package gpbft_test

import (
	"bytes"
	"encoding/binary"
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
			wantString: "@0",
		},
		{
			name:       "ZeroTipSet is zero",
			subject:    gpbft.ZeroTipSet(),
			wantZero:   true,
			wantString: "@0",
		},
		{
			name:       "NewTipSet with zero values is zero",
			subject:    gpbft.NewTipSet(0, gpbft.NewTipSetID(nil)),
			wantZero:   true,
			wantString: "@0",
		},
		{
			name:       "Non-zero is not zero",
			subject:    gpbft.NewTipSet(1413, gpbft.NewTipSetID([]byte("fish"))),
			wantString: "fish@1413",
		},
		{
			name:       "negative epoch is accepted",
			subject:    gpbft.NewTipSet(-1413, gpbft.NewTipSetID([]byte("fish"))),
			wantString: "fish@-1413",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantZero, test.subject.IsZero())
			require.Equal(t, test.wantString, test.subject.String())
			requireTipSetMarshaledForSigning(t, test.subject)
		})
	}
}

func requireTipSetMarshaledForSigning(t *testing.T, subject gpbft.TipSet) {
	t.Helper()
	var gotSigningMarshal bytes.Buffer
	subject.MarshalForSigning(&gotSigningMarshal)
	wantPrefix := binary.BigEndian.AppendUint64(nil, uint64(subject.Epoch))
	wantSuffix := subject.ID.Bytes()
	require.Equal(t, len(wantPrefix)+len(wantSuffix), gotSigningMarshal.Len())
	require.True(t, bytes.HasPrefix(gotSigningMarshal.Bytes(), wantPrefix))
	require.True(t, bytes.HasSuffix(gotSigningMarshal.Bytes(), wantSuffix))
}

func TestECChain(t *testing.T) {
	t.Parallel()

	zeroTipSet := gpbft.ZeroTipSet()
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
		require.Equal(t, zeroTipSet, subject.HeadOrZero())
		require.NoError(t, subject.Validate())
		requireMonotonicallyIncreasingEpochs(t, subject)
	})
	t.Run("NewChain with zero-value base is error", func(t *testing.T) {
		subject, err := gpbft.NewChain(zeroTipSet)
		require.Error(t, err)
		require.Nil(t, subject)
	})
	t.Run("NewChain with repeated epochs is error", func(t *testing.T) {
		tipSet := gpbft.NewTipSet(1413, gpbft.ZeroTipSetID())
		subject, err := gpbft.NewChain(tipSet, tipSet)
		require.Error(t, err)
		require.Nil(t, subject)
	})
	t.Run("NewChain with epoch gaps is not error", func(t *testing.T) {
		subject, err := gpbft.NewChain(gpbft.NewTipSet(1413, gpbft.ZeroTipSetID()), gpbft.NewTipSet(1414, gpbft.ZeroTipSetID()))
		require.NoError(t, err)
		require.NoError(t, subject.Validate())
		requireMonotonicallyIncreasingEpochs(t, subject)
	})
	t.Run("NewChain with unordered tipSets is error", func(t *testing.T) {
		subject, err := gpbft.NewChain(gpbft.NewTipSet(2, gpbft.ZeroTipSetID()), zeroTipSet)
		require.Error(t, err)
		require.Nil(t, subject)
	})
	t.Run("NewChain with ordered duplicate epoch is error", func(t *testing.T) {
		subject, err := gpbft.NewChain(zeroTipSet,
			gpbft.NewTipSet(2, gpbft.ZeroTipSetID()),
			gpbft.NewTipSet(2, gpbft.NewTipSetID([]byte("fish"))),
			gpbft.NewTipSet(2, gpbft.NewTipSetID([]byte("lobster"))))
		require.Error(t, err)
		require.Nil(t, subject)
		require.NoError(t, subject.Validate())
		requireMonotonicallyIncreasingEpochs(t, subject)
	})
	t.Run("extended chain is as expected", func(t *testing.T) {
		wantBase := gpbft.NewTipSet(1413, gpbft.NewTipSetID([]byte("fish")))
		subject, err := gpbft.NewChain(wantBase)
		require.NoError(t, err)
		require.Len(t, subject, 1)
		require.Equal(t, wantBase, subject.Base())
		require.Equal(t, wantBase, subject.Head())
		require.Equal(t, wantBase, subject.HeadOrZero())
		require.NoError(t, subject.Validate())
		requireMonotonicallyIncreasingEpochs(t, subject)

		wantNextID := gpbft.NewTipSetID([]byte("lobster"))
		wantNextTipSet := gpbft.NewTipSet(wantBase.Epoch+1, wantNextID)
		subjectExtended := subject.Extend(wantNextID)
		require.Len(t, subjectExtended, 2)
		require.NoError(t, subjectExtended.Validate())
		requireMonotonicallyIncreasingEpochs(t, subjectExtended)
		require.Equal(t, wantBase, subjectExtended.Base())
		require.Equal(t, []gpbft.TipSet{wantNextTipSet}, subjectExtended.Suffix())
		require.Equal(t, wantNextTipSet, subjectExtended.Head())
		require.Equal(t, wantNextTipSet, subjectExtended.HeadOrZero())
		require.Equal(t, wantNextTipSet, subjectExtended.Prefix(1).Head())
		require.True(t, subjectExtended.HasTipset(gpbft.NewTipSet(wantBase.Epoch+1, wantNextID)))
		require.False(t, subject.HasPrefix(subjectExtended))
		require.True(t, subjectExtended.HasPrefix(subject))

		require.False(t, subject.Extend(wantBase.ID).HasPrefix(subjectExtended.Extend(wantNextID)))
	})
	t.Run("SameBase is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain(gpbft.NewTipSet(2, gpbft.ZeroTipSetID()))
		require.NoError(t, err)
		require.False(t, nonZeroChain.SameBase(zeroChain))
		require.False(t, zeroChain.SameBase(nonZeroChain))
		require.False(t, zeroChain.SameBase(zeroChain))
	})
	t.Run("HasPrefix is false when either chain is zero", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		nonZeroChain, err := gpbft.NewChain(gpbft.NewTipSet(2, gpbft.ZeroTipSetID()))
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
		subject := gpbft.ECChain{zeroTipSet, gpbft.NewTipSet(1, gpbft.ZeroTipSetID())}
		require.Error(t, subject.Validate())
	})
	t.Run("unordered chain is invalid", func(t *testing.T) {
		subject := gpbft.ECChain{gpbft.NewTipSet(2, gpbft.ZeroTipSetID()), gpbft.NewTipSet(1, gpbft.ZeroTipSetID())}
		require.Error(t, subject.Validate())
	})
}

func requireMonotonicallyIncreasingEpochs(t *testing.T, subject gpbft.ECChain) {
	t.Helper()
	var latestEpoch int64
	for index, tipSet := range subject {
		require.Less(t, latestEpoch, tipSet.Epoch, "not monotonically increasing at index %d", index)
		latestEpoch = tipSet.Epoch
	}
}
