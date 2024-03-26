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
	wantSuffix := subject.CID.Bytes()
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
	})
	// TODO: test chain invariant properties as per:
	//        - https://github.com/filecoin-project/go-f3/issues/128
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
	})
	t.Run("extended chain is as expected", func(t *testing.T) {
		wantBase := gpbft.NewTipSet(1413, gpbft.NewTipSetID([]byte("fish")))
		subject, err := gpbft.NewChain(wantBase)
		require.NoError(t, err)
		require.Len(t, subject, 1)
		require.Equal(t, wantBase, subject.Base())
		require.Equal(t, wantBase, subject.Head())
		require.Equal(t, wantBase, subject.HeadOrZero())

		wantNextID := gpbft.NewTipSetID([]byte("lobster"))
		wantNextTipSet := gpbft.NewTipSet(wantBase.Epoch+1, wantNextID)
		subjectExtended := subject.Extend(wantNextID)
		require.Len(t, subjectExtended, 2)
		require.Equal(t, wantBase, subjectExtended.Base())
		require.Equal(t, []gpbft.TipSet{wantNextTipSet}, subjectExtended.Suffix())
		require.Equal(t, wantNextTipSet, subjectExtended.Head())
		require.Equal(t, wantNextTipSet, subjectExtended.HeadOrZero())
		require.Equal(t, wantNextTipSet, subjectExtended.Prefix(1).Head())
		require.True(t, subjectExtended.HasTipset(gpbft.NewTipSet(wantBase.Epoch+1, wantNextID)))
		require.False(t, subject.HasPrefix(subjectExtended))
		require.True(t, subjectExtended.HasPrefix(subject))

		require.False(t, subject.Extend(wantBase.CID).HasPrefix(subjectExtended.Extend(wantNextID)))
	})

}
