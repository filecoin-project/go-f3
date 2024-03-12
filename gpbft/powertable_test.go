package gpbft_test

import (
	"sort"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestPowerTable(t *testing.T) {

	var (
		oneValidEntry        = gpbft.PowerEntry{ID: 1413, Power: gpbft.NewStoragePower(1414000), PubKey: []byte("fish")}
		anotherValidEntry    = gpbft.PowerEntry{ID: 1513, Power: gpbft.NewStoragePower(1514000), PubKey: []byte("lobster")}
		yetAnotherValidEntry = gpbft.PowerEntry{ID: 1490, Power: gpbft.NewStoragePower(1491000), PubKey: []byte("lobster")}
		zeroPowerEntry       = gpbft.PowerEntry{ID: 1613, Power: gpbft.NewStoragePower(0), PubKey: []byte("fish")}
		noPubKeyEntry        = gpbft.PowerEntry{ID: 1613, Power: gpbft.NewStoragePower(1614000)}
	)

	t.Run("empty table", func(t *testing.T) {
		t.Run("is valid", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.NoError(t, subject.Validate())
		})
		t.Run("gets nil", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			_, gotPower, gotKey := subject.Get(1413)
			require.Nil(t, gotPower)
			require.Nil(t, gotKey)
		})
	})
	t.Run("on Add", func(t *testing.T) {
		t.Run("valid entry is added", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.NoError(t, subject.Add(oneValidEntry))
			require.NoError(t, subject.Validate())
			requireAddedToPowerTable(t, subject, oneValidEntry)
			require.NoError(t, subject.Validate())
		})
		t.Run("table stays in order", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.NoError(t, subject.Add(oneValidEntry, anotherValidEntry))
			require.NoError(t, subject.Validate())
			requireAddedToPowerTable(t, subject, oneValidEntry)
			requireAddedToPowerTable(t, subject, anotherValidEntry)
			require.True(t, sort.IsSorted(subject))
			require.NoError(t, subject.Validate())

			require.NoError(t, subject.Add(yetAnotherValidEntry))
			requireAddedToPowerTable(t, subject, yetAnotherValidEntry)
			require.True(t, sort.IsSorted(subject))
			require.NoError(t, subject.Validate())
		})
		t.Run("duplicate entry is error", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.ErrorContains(t, subject.Add(oneValidEntry, oneValidEntry), "already exists")
		})
		t.Run("zero power entry is error", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.ErrorContains(t, subject.Add(zeroPowerEntry), "zero power")
		})
		t.Run("no pub key power entry is error", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			require.ErrorContains(t, subject.Add(noPubKeyEntry), "public key")
		})
		t.Run("same power is ordered by ID", func(t *testing.T) {
			subject := gpbft.NewPowerTable()
			samePowerEntryWithSmallerID := gpbft.PowerEntry{
				ID:     oneValidEntry.ID - 1,
				Power:  oneValidEntry.Power,
				PubKey: []byte("barreleye"),
			}
			require.NoError(t, subject.Add(oneValidEntry, samePowerEntryWithSmallerID))
			requireAddedToPowerTable(t, subject, oneValidEntry)
			requireAddedToPowerTable(t, subject, samePowerEntryWithSmallerID)
			require.Equal(t, samePowerEntryWithSmallerID, subject.Entries[0])
			require.Equal(t, oneValidEntry, subject.Entries[1])
			require.NoError(t, subject.Validate())
		})
	})
	t.Run("on Validate", func(t *testing.T) {
		tests := []struct {
			name    string
			subject func() *gpbft.PowerTable
			wantErr string
		}{
			{
				name: "missing lookup map is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					subject.Entries = append(subject.Entries, oneValidEntry)
					return subject
				},
				wantErr: "inconsistent entries",
			},
			{
				name: "inconsistent lookup map is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					require.NoError(t, subject.Add(oneValidEntry))
					subject.Lookup[oneValidEntry.ID] = 14
					return subject
				},
				wantErr: "lookup index does not match",
			},
			{
				name: "incorrect total is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					require.NoError(t, subject.Add(oneValidEntry, anotherValidEntry))
					subject.Total.Sub(subject.Total, gpbft.NewStoragePower(1))
					return subject
				},
				wantErr: "total power does not match",
			},
			{
				name: "zero power entry is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					subject.Entries = append(subject.Entries, zeroPowerEntry)
					subject.ScaledPower = append(subject.ScaledPower, 0)
					subject.Lookup[zeroPowerEntry.ID] = 0
					return subject
				},
				wantErr: "zero power",
			},
			{
				name: "no pub key is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					subject.Entries = append(subject.Entries, noPubKeyEntry)
					subject.ScaledPower = append(subject.ScaledPower, 0)
					subject.Lookup[noPubKeyEntry.ID] = 0
					subject.Total.Add(subject.Total, noPubKeyEntry.Power)
					return subject
				},
				wantErr: "unspecified public key",
			},
			{
				name: "unordered is error",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					require.NoError(t, subject.Add(oneValidEntry, anotherValidEntry, yetAnotherValidEntry))
					subject.Swap(0, 2)
					return subject
				},
				wantErr: "not in order",
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				subject := test.subject()
				gotErr := subject.Validate()
				if test.wantErr != "" {
					require.ErrorContains(t, gotErr, test.wantErr)
				} else {
					require.NoError(t, gotErr)
				}
			})
		}
	})
	t.Run("on Copy", func(t *testing.T) {
		tests := []struct {
			name    string
			subject func() *gpbft.PowerTable
		}{
			{
				name: "empty table is copied",
				subject: func() *gpbft.PowerTable {
					return gpbft.NewPowerTable()
				},
			},
			{
				name: "non-empty is copied",
				subject: func() *gpbft.PowerTable {
					subject := gpbft.NewPowerTable()
					require.NoError(t, subject.Add(oneValidEntry, anotherValidEntry, yetAnotherValidEntry))
					return subject
				},
			},
		}
		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				subject := test.subject()
				gotCopy := subject.Copy()
				require.Equal(t, subject, gotCopy)
				require.NotSame(t, subject, gotCopy)
			})
		}
	})
}

func requireAddedToPowerTable(t *testing.T, subject *gpbft.PowerTable, entry gpbft.PowerEntry) {
	t.Helper()
	require.True(t, subject.Has(entry.ID))
	_, gotPower, gotKey := subject.Get(entry.ID)
	require.Equal(t, entry.Power, gotPower)
	require.Equal(t, entry.PubKey, gotKey)
}
