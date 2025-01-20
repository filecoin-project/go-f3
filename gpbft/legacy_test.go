package gpbft_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestLegacyECChain_Marshaling(t *testing.T) {
	var (
		tipset1       = gpbft.TipSet{Epoch: 0, Key: gpbft.MakeCid([]byte("fish")).Bytes(), PowerTable: gpbft.MakeCid([]byte("lobster"))}
		tipset2       = gpbft.TipSet{Epoch: 1, Key: gpbft.MakeCid([]byte("fishmuncher")).Bytes(), PowerTable: gpbft.MakeCid([]byte("lobstergobler"))}
		subject       = gpbft.ECChain{TipSets: []*gpbft.TipSet{&tipset1, &tipset2}}
		legacySubject = gpbft.LegacyECChain{tipset1, tipset2}
	)
	t.Run("CBOR/to legacy", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, subject.MarshalCBOR(&buf))

		var asOldFormat gpbft.LegacyECChain
		require.NoError(t, asOldFormat.UnmarshalCBOR(&buf))
		require.Equal(t, subject.Len(), len(asOldFormat))
		for i, want := range subject.TipSets {
			got := &asOldFormat[i]
			require.True(t, want.Equal(got))
		}
	})
	t.Run("CBOR/from legacy", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, legacySubject.MarshalCBOR(&buf))

		var asNewFormat gpbft.ECChain
		require.NoError(t, asNewFormat.UnmarshalCBOR(&buf))
		require.Equal(t, len(legacySubject), asNewFormat.Len())
		for i, want := range subject.TipSets {
			got := asNewFormat.TipSets[i]
			require.True(t, want.Equal(got))
		}
	})
	t.Run("JSON/to legacy", func(t *testing.T) {
		data, err := json.Marshal(&subject)
		require.NoError(t, err)

		var asOldFormat gpbft.LegacyECChain
		require.NoError(t, json.Unmarshal(data, &asOldFormat))
		for i, want := range subject.TipSets {
			got := &asOldFormat[i]
			require.True(t, want.Equal(got))
		}
	})
	t.Run("JSON/from legacy", func(t *testing.T) {
		data, err := json.Marshal(legacySubject)
		require.NoError(t, err)

		var asNewFormat gpbft.ECChain
		require.NoError(t, json.Unmarshal(data, &asNewFormat))
		for i, want := range subject.TipSets {
			got := asNewFormat.TipSets[i]
			require.True(t, want.Equal(got))
		}
	})
}
