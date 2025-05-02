package ec_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/consensus"

	"github.com/stretchr/testify/require"
)

var powerTableA = gpbft.PowerEntries{
	{ID: 1, Power: gpbft.NewStoragePower(50)},
	{ID: 2, Power: gpbft.NewStoragePower(30)},
	{ID: 3, Power: gpbft.NewStoragePower(29)},
}

var powerTableB = gpbft.PowerEntries{
	{ID: 3, Power: gpbft.NewStoragePower(10)},
	{ID: 4, Power: gpbft.NewStoragePower(4)},
	{ID: 2, Power: gpbft.NewStoragePower(0)},
}

var powerTableC = gpbft.PowerEntries{
	{ID: 1, Power: gpbft.NewStoragePower(50)},
	{ID: 3, Power: gpbft.NewStoragePower(10)},
	{ID: 4, Power: gpbft.NewStoragePower(4)},
}

func TestReplacePowerTable(t *testing.T) {
	backend := consensus.NewFakeEC(consensus.WithInitialPowerTable(powerTableA))
	modifiedBackend := ec.WithModifiedPower(backend, powerTableB, true)

	head, err := modifiedBackend.GetHead(context.Background())
	require.NoError(t, err)

	// Replaces the power table, but doesn't return the "0" entries.
	pt, err := modifiedBackend.GetPowerTable(context.Background(), head.Key())
	require.NoError(t, err)
	require.EqualValues(t, powerTableB[:2], pt)
}

func TestModifyPowerTable(t *testing.T) {
	backend := consensus.NewFakeEC(consensus.WithInitialPowerTable(powerTableA))
	modifiedBackend := ec.WithModifiedPower(backend, powerTableB, false)

	head, err := modifiedBackend.GetHead(context.Background())
	require.NoError(t, err)

	pt, err := modifiedBackend.GetPowerTable(context.Background(), head.Key())
	require.NoError(t, err)
	require.EqualValues(t, powerTableC, pt)
}

func TestBypassModifiedPowerTable(t *testing.T) {
	backend := consensus.NewFakeEC(consensus.WithInitialPowerTable(powerTableA))
	modifiedBackend := ec.WithModifiedPower(backend, nil, false)
	require.Equal(t, backend, modifiedBackend)
}
