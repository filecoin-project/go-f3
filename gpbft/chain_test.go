package gpbft_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/merkle"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestECChain(t *testing.T) {
	t.Parallel()

	ptCid := gpbft.MakeCid([]byte("pt"))
	zeroTipSet := &gpbft.TipSet{}
	oneTipSet := &gpbft.TipSet{Epoch: 0, Key: []byte{1}, PowerTable: ptCid}
	t.Run("zero-value is zero", func(t *testing.T) {
		var subject *gpbft.ECChain
		require.True(t, subject.IsZero())
		require.Zero(t, subject.Len())
		require.Equal(t, (&gpbft.ECChain{}).Key(), subject.Key())
		require.False(t, subject.HasBase(zeroTipSet))
		require.Nil(t, subject.BaseChain())
		require.True(t, subject.BaseChain().IsZero())
		require.True(t, subject.Eq(subject))
		require.True(t, subject.Eq(nil))
		require.Nil(t, subject.Suffix())
		require.Nil(t, subject.Prefix(0))
		require.Nil(t, subject.Base())
		require.Nil(t, subject.Head())
		require.NoError(t, subject.Validate())

		// A nil chain and an empty chain are both zero and therefore should be equal.
		require.True(t, subject.Eq(new(gpbft.ECChain)))
	})
	t.Run("NewChain with zero-value base is error", func(t *testing.T) {
		subject, err := gpbft.NewChain(zeroTipSet)
		require.Error(t, err)
		require.Nil(t, subject)
	})
	t.Run("extended chain is as expected", func(t *testing.T) {
		wantBase := &gpbft.TipSet{Epoch: 0, Key: []byte("fish"), PowerTable: ptCid}
		subject, err := gpbft.NewChain(wantBase)
		require.NoError(t, err)
		require.Equal(t, subject.Len(), 1)
		require.Equal(t, wantBase, subject.Base())
		require.True(t, subject.Eq(subject.BaseChain()))
		require.Equal(t, wantBase, subject.Head())
		require.False(t, subject.HasSuffix())
		require.NoError(t, subject.Validate())

		wantNext := &gpbft.TipSet{Epoch: 1, Key: []byte("lobster"), PowerTable: ptCid}
		subjectExtended := subject.Extend(wantNext.Key)
		require.Equal(t, subjectExtended.Len(), 2)
		require.NoError(t, subjectExtended.Validate())
		require.Equal(t, wantBase, subjectExtended.Base())
		require.Equal(t, []*gpbft.TipSet{wantNext}, subjectExtended.Suffix())
		require.Equal(t, wantNext, subjectExtended.Head())
		require.True(t, subjectExtended.HasSuffix())
		require.Equal(t, wantNext, subjectExtended.Prefix(1).Head())
		require.True(t, subjectExtended.HasPrefix(subject))
		require.False(t, subject.Extend(wantBase.Key).HasPrefix(subjectExtended.Extend(wantNext.Key)))
	})
	t.Run("zero-valued chain is valid", func(t *testing.T) {
		var zeroChain gpbft.ECChain
		require.NoError(t, zeroChain.Validate())
	})
	t.Run("ordered chain with zero-valued base is invalid", func(t *testing.T) {
		subject := gpbft.ECChain{TipSets: []*gpbft.TipSet{oneTipSet, zeroTipSet}}
		require.Error(t, subject.Validate())
	})
	t.Run("ordered but negative epoch is invalid", func(t *testing.T) {
		subject := gpbft.ECChain{TipSets: []*gpbft.TipSet{{
			Epoch:       -1,
			Key:         oneTipSet.Key,
			PowerTable:  oneTipSet.PowerTable,
			Commitments: oneTipSet.Commitments,
		}, oneTipSet}}
		require.Error(t, subject.Validate())
	})
	t.Run("too long a chain is invalid", func(t *testing.T) {
		var subject gpbft.ECChain
		subject.TipSets = make([]*gpbft.TipSet, gpbft.ChainMaxLen+3)
		for i := range subject.TipSets {
			subject.TipSets[i] = &gpbft.TipSet{Epoch: int64(i), Key: []byte{byte(i)}, PowerTable: ptCid}
			require.NoError(t, subject.TipSets[i].Validate())
		}
		require.Error(t, subject.Validate())
	})
	t.Run("prefix and extend don't mutate", func(t *testing.T) {
		subject := &gpbft.ECChain{TipSets: []*gpbft.TipSet{
			{Epoch: 0, Key: []byte{0}, PowerTable: ptCid},
			{Epoch: 1, Key: []byte{1}, PowerTable: ptCid},
		}}
		dup := subject.Prefix(subject.Len())
		after := subject.Prefix(0).Extend([]byte{2})
		require.True(t, subject.Eq(dup))
		require.True(t, after.Eq(&gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: []byte{0}, PowerTable: ptCid},
				{Epoch: 1, Key: []byte{2}, PowerTable: ptCid},
			},
		}))
	})
	t.Run("extending multiple times doesn't clobber", func(t *testing.T) {
		// simulate over-allocation
		initial := &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: []byte{0}},
				{},
			}[:1],
		}

		first := initial.Extend([]byte{1})
		second := initial.Extend([]byte{2})
		require.Equal(t, first.TipSets[1], &gpbft.TipSet{Epoch: 1, Key: []byte{1}})
		require.Equal(t, second.TipSets[1], &gpbft.TipSet{Epoch: 1, Key: []byte{2}})
	})
	t.Run("appending multiple times doesn't clobber", func(t *testing.T) {
		var (
			wantBase         = &gpbft.TipSet{Epoch: 0, Key: []byte{0}}
			wantFirstSuffix  = &gpbft.TipSet{Epoch: 1, Key: []byte{1}}
			wantSecondSuffix = &gpbft.TipSet{Epoch: 2, Key: []byte{2}}
		)
		// simulate over-allocation
		initial := &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				wantBase,
				{},
			}[:1],
		}

		first := initial.Append(wantFirstSuffix)
		require.Equal(t, first.TipSets[1], wantFirstSuffix)
		require.Equal(t, 2, first.Len())

		second := initial.Append(wantSecondSuffix)
		require.Equal(t, second.TipSets[1], wantSecondSuffix)
		require.Equal(t, 2, second.Len())

		require.Equal(t, initial.TipSets[0], wantBase)
		require.Equal(t, 1, initial.Len())
	})
	t.Run("key calculation is not racy", func(t *testing.T) {
		subject := &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{oneTipSet},
		}
		// Calculate key from a copy of the chain for consistency checking.
		subjectCopy := &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{oneTipSet},
		}
		require.True(t, subject.Eq(subjectCopy))
		require.Equal(t, subject, subjectCopy)

		wantKey := subjectCopy.Key()
		var eg errgroup.Group
		for range 8 {
			eg.Go(func() error {
				if wantKey != subject.Key() {
					return errors.New("key mismatch")
				}
				return nil
			})
		}
		require.NoError(t, eg.Wait())
	})
	t.Run("marshals as array in JSON", func(t *testing.T) {
		subject := &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{
				{Epoch: 0, Key: gpbft.MakeCid([]byte("fish")).Bytes(), PowerTable: gpbft.MakeCid([]byte("lbster"))},
			},
		}
		data, err := json.Marshal(subject)
		require.NoError(t, err)

		var azSlice []*gpbft.TipSet
		require.NoError(t, json.Unmarshal(data, &azSlice))
		require.Equal(t, subject.TipSets, azSlice)

		var azStruct gpbft.ECChain
		require.NoError(t, json.Unmarshal(data, &azStruct))
		require.True(t, subject.Eq(&azStruct))
	})
}

func TestECChainKeysForPrefixes(t *testing.T) {
	t.Parallel()

	pt1Cid := gpbft.MakeCid([]byte("pt1"))
	pt2Cid := gpbft.MakeCid([]byte("pt2"))
	pt3Cid := gpbft.MakeCid([]byte("pt3"))
	tipSets := []*gpbft.TipSet{
		{Epoch: 0, Key: []byte{0}, PowerTable: pt1Cid},
		{Epoch: 1, Key: []byte{1}, PowerTable: pt2Cid},
		{Epoch: 2, Key: []byte{2}, PowerTable: pt3Cid},
	}

	chain := &gpbft.ECChain{TipSets: tipSets}
	keys := chain.KeysForPrefixes()

	require.Equal(t, len(keys), len(tipSets), "KeysForPrefixes should return a key for each prefix")

	for i, key := range keys {
		prefixChain := chain.Prefix(i)
		require.Equal(t, key, prefixChain.Key(), "Key for prefix %d should match", i)
	}
}

func TestECChainAllPrefixes(t *testing.T) {
	t.Parallel()

	pt1Cid := gpbft.MakeCid([]byte("pt1"))
	pt2Cid := gpbft.MakeCid([]byte("pt2"))
	pt3Cid := gpbft.MakeCid([]byte("pt3"))
	tipSets := []*gpbft.TipSet{
		{Epoch: 0, Key: []byte{0}, PowerTable: pt1Cid},
		{Epoch: 1, Key: []byte{1}, PowerTable: pt2Cid},
		{Epoch: 2, Key: []byte{2}, PowerTable: pt3Cid},
	}

	chain := &gpbft.ECChain{TipSets: tipSets}
	prefixes := chain.AllPrefixes()

	require.Equal(t, len(prefixes), len(tipSets), "AllPrefixes should return a prefix for each tipset")

	for i, prefix := range prefixes {
		require.Equal(t, prefix.Len(), i+1, "Prefix %d should have length %d", i, i+1)
		expected := chain.Prefix(i)
		require.True(t, expected.Eq(prefix))
		require.Equal(t, expected.Key(), prefix.Key())
		require.NotSame(t, expected, prefix)
	}
}

func TestECChain_Eq(t *testing.T) {
	t.Parallel()
	var (
		commitThis              = [32]byte{0x01}
		commitThat              = [32]byte{0x02}
		ptThis                  = gpbft.MakeCid([]byte("fish"))
		ptThat                  = gpbft.MakeCid([]byte("lobster"))
		ts1                     = &gpbft.TipSet{Epoch: 1, Key: []byte("barreleye1"), PowerTable: ptThat, Commitments: commitThis}
		ts2                     = &gpbft.TipSet{Epoch: 2, Key: []byte("barreleye2"), PowerTable: ptThat, Commitments: commitThis}
		ts3                     = &gpbft.TipSet{Epoch: 3, Key: []byte("barreleye3"), PowerTable: ptThat, Commitments: commitThis}
		ts1DifferentCommitments = &gpbft.TipSet{1, []byte("barreleye1"), ptThat, commitThat}
		ts1DifferentPowerTable  = &gpbft.TipSet{1, []byte("barreleye1"), ptThis, commitThis}
	)
	for _, tt := range []struct {
		name   string
		one    *gpbft.ECChain
		other  *gpbft.ECChain
		expect bool
	}{
		{
			name:   "Nil chains",
			expect: true,
		},
		{
			name:   "Equal chains",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			expect: true,
		},
		{
			name:   "Different chains",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts3}},
			expect: false,
		},
		{
			name:   "Same chain compared with itself",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			expect: true,
		},
		{
			name:   "Different lengths",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1, ts2}},
			expect: false,
		},
		{
			name:   "Zero chains (empty chains)",
			one:    &gpbft.ECChain{},
			other:  &gpbft.ECChain{},
			expect: true,
		},
		{
			name:   "One zero chain",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1}},
			other:  &gpbft.ECChain{},
			expect: false,
		},
		{
			name:   "Different commitments",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1DifferentCommitments}},
			expect: false,
		},
		{
			name:   "Different power table",
			one:    &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1}},
			other:  &gpbft.ECChain{TipSets: []*gpbft.TipSet{ts1DifferentPowerTable}},
			expect: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expect, tt.one.Eq(tt.other), "Unexpected equality result for one compared to other: %s", tt.name)
			assert.Equal(t, tt.expect, tt.other.Eq(tt.one), "Unexpected equality result for other compared to one: %s", tt.name)
		})
	}
}

func TestTipSetSerialization(t *testing.T) {
	t.Parallel()
	var (
		c1        = gpbft.MakeCid([]byte("barreleye1"))
		c2        = gpbft.MakeCid([]byte("barreleye2"))
		c3        = gpbft.MakeCid([]byte("barreleye3"))
		testCases = []gpbft.TipSet{
			{
				Epoch:       1,
				Key:         append(append(c1.Bytes(), c2.Bytes()...), c3.Bytes()...),
				PowerTable:  gpbft.MakeCid([]byte("fish")),
				Commitments: [32]byte{0x01},
			},
			{
				Epoch:       101,
				Key:         c1.Bytes(),
				PowerTable:  gpbft.MakeCid([]byte("lobster")),
				Commitments: [32]byte{0x02},
			},
		}
		badJsonEncodable = []struct {
			ts  gpbft.TipSet
			err string
		}{
			{
				ts: gpbft.TipSet{
					Epoch:       1,
					Key:         []byte("nope"),
					PowerTable:  gpbft.MakeCid([]byte("fish")),
					Commitments: [32]byte{0x01},
				},
				err: "invalid cid",
			},
		}
		badJsonDecodable = []struct {
			json string
			err  string
		}{
			{
				json: `{"Key":["nope"],"Commitments":"AgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","Epoch":101,"PowerTable":{"/":"bafy2bzaced5zqzzbxzyzuq2tcxhuclnvdn3y6ijhurgaapnbayul2dd5gspc4"}}`,
				err:  "invalid cid",
			},
			{
				json: `{"Key":[{"/":"bafy2bzacecp4qqs334yrvzxsnlolskbtvyc3ub7k5tzx4s2m77vimzzkduj3g"}],"Commitments":"bm9wZQ==","Epoch":101,"PowerTable":{"/":"bafy2bzaced5zqzzbxzyzuq2tcxhuclnvdn3y6ijhurgaapnbayul2dd5gspc4"}}`,
				err:  "32 bytes",
			},
		}
	)

	t.Run("cbor round trip", func(t *testing.T) {
		req := require.New(t)
		for _, ts := range testCases {
			var buf bytes.Buffer
			req.NoError(ts.MarshalCBOR(&buf))
			t.Logf("cbor: %x", buf.Bytes())
			var rt gpbft.TipSet
			req.NoError(rt.UnmarshalCBOR(&buf))
			req.Equal(ts, rt)
		}
	})

	t.Run("json round trip", func(t *testing.T) {
		req := require.New(t)
		for _, ts := range testCases {
			data, err := ts.MarshalJSON()
			req.NoError(err)
			t.Logf("json: %s", data)
			var rt gpbft.TipSet
			req.NoError(rt.UnmarshalJSON(data))
			req.Equal(ts, rt)

			// check that we serialized the CIDs in the standard dag-json form
			var bareMap map[string]any
			req.NoError(json.Unmarshal(data, &bareMap))
			keyField, ok := bareMap["Key"].([]any)
			req.True(ok)
			req.Len(keyField, len(ts.Key)/38)
			for j, c := range []cid.Cid{c1, c2, c3}[:len(ts.Key)/38] {
				req.Equal(map[string]any{"/": c.String()}, keyField[j])
			}

			// check that the supplemental data is a base64 string
			commitField, ok := bareMap["Commitments"].(string)
			req.True(ok)
			req.Len(commitField, 44)
		}
	})

	t.Run("json error cases", func(t *testing.T) {
		req := require.New(t)
		for i, tc := range badJsonEncodable {
			_, err := tc.ts.MarshalJSON()
			req.ErrorContains(err, tc.err, "expected error for test case %d", i)
		}
		for i, tc := range badJsonDecodable {
			var ts gpbft.TipSet
			err := ts.UnmarshalJSON([]byte(tc.json))
			req.ErrorContains(err, tc.err, "expected error for test case %d", i)
		}
	})
}

func TestECChainKey(t *testing.T) {
	t.Parallel()
	requireConsistentJSONMarshalling := func(t *testing.T, subject gpbft.ECChainKey) {
		var fromJson gpbft.ECChainKey
		asJson, err := json.Marshal(subject)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(asJson, &fromJson))
		require.Equal(t, subject, fromJson)
	}
	t.Run("zero", func(t *testing.T) {
		var subject gpbft.ECChainKey
		require.True(t, subject.IsZero())
		require.Equal(t, len(subject), merkle.DigestLength)
		requireConsistentJSONMarshalling(t, subject)
	})
	t.Run("non-zero", func(t *testing.T) {
		subject := gpbft.ECChainKey([]byte("barreleye undadasea lookin at me"))
		require.False(t, subject.IsZero())
		require.Equal(t, len(subject), merkle.DigestLength)
		require.Equal(t, merkle.DigestLength, len(subject))
		requireConsistentJSONMarshalling(t, subject)
	})
}
