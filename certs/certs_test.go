package certs_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/signing"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/require"
)

const networkName = "f3test"

func TestNoFinalityCertificates(t *testing.T) {
	backend := signing.NewFakeBackend()
	nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, nil, 0, nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, nextInstance)
	require.Empty(t, chain)
	require.Empty(t, newPowerTable)
}

func TestPowerTableDiff(t *testing.T) {
	backend := signing.NewFakeBackend()
	powerTable := randomPowerTable(backend, 100)
	// Empty
	{
		require.Empty(t, certs.MakePowerTableDiff(nil, nil))
		require.Empty(t, certs.MakePowerTableDiff(powerTable, powerTable))
	}

	// Add all, remove all.
	{
		expDeltaRemove := make(certs.PowerTableDiff, len(powerTable))
		expDeltaAdd := make(certs.PowerTableDiff, len(powerTable))
		for i, e := range powerTable {
			expDeltaAdd[i] = certs.PowerTableDelta{
				ParticipantID: e.ID,
				PowerDelta:    e.Power,
				SigningKey:    e.PubKey,
			}

			expDeltaRemove[i] = certs.PowerTableDelta{
				ParticipantID: e.ID,
				PowerDelta:    e.Power.Neg(),
			}

		}
		require.Equal(t, expDeltaRemove, certs.MakePowerTableDiff(powerTable, nil))
		require.Equal(t, expDeltaAdd, certs.MakePowerTableDiff(nil, powerTable))

		testRoundTrip(t, expDeltaRemove)
		testRoundTrip(t, expDeltaAdd)

		addAll, err := certs.ApplyPowerTableDiffs(nil, expDeltaAdd)
		require.NoError(t, err)
		require.Equal(t, powerTable, addAll)
		remAll, err := certs.ApplyPowerTableDiffs(powerTable, expDeltaRemove)
		require.NoError(t, err)
		require.Empty(t, remAll)

		// Deltas must be ordered
		slices.Reverse(expDeltaAdd)
		_, err = certs.ApplyPowerTableDiffs(nil, expDeltaAdd)
		require.ErrorContains(t, err, "not sorted")
	}

	{
		// randomized
		rng := rand.New(rand.NewSource(1234))
		maxPower := int64(len(powerTable) * 2)
		var removedPowerEntries gpbft.PowerEntries
		powerTables := make([]gpbft.PowerEntries, 100)
		for i := range powerTables {
			powerTable, removedPowerEntries = randomizePowerTable(rng, backend, maxPower, powerTable, removedPowerEntries)
			powerTables[i] = powerTable
		}
		for i := 0; i < 100; i++ {
			a := powerTables[rng.Intn(len(powerTables))]
			b := powerTables[rng.Intn(len(powerTables))]
			diffAB := certs.MakePowerTableDiff(a, b)
			diffBA := certs.MakePowerTableDiff(b, a)

			bNew, err := certs.ApplyPowerTableDiffs(a, diffAB)
			require.NoError(t, err)
			require.Equal(t, b, bNew)

			aNew, err := certs.ApplyPowerTableDiffs(b, diffBA)
			require.NoError(t, err)
			require.Equal(t, a, aNew)
		}
	}

	// explicit transitions
	{
		// Add power
		newPowerTable, err := certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(1),
		}})
		require.NoError(t, err)
		require.Equal(t,
			big.Sub(newPowerTable[0].Power, powerTable[0].Power),
			gpbft.NewStoragePower(1),
		)

		// Add power with key.
		newPowerTable, err = certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(1),
			SigningKey:    powerTable[1].PubKey,
		}})
		require.NoError(t, err)
		require.Equal(t,
			big.Sub(newPowerTable[0].Power, powerTable[0].Power),
			gpbft.NewStoragePower(1),
		)
		require.Equal(t, newPowerTable[0].PubKey, powerTable[1].PubKey)

		// Change Key without power
		newPowerTable, err = certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(0),
			SigningKey:    powerTable[1].PubKey,
		}})
		require.NoError(t, err)
		require.Equal(t, newPowerTable[0].PubKey, powerTable[1].PubKey)
	}

	// invalid transitions
	{
		// Idempotent key change
		_, err := certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(1),
			SigningKey:    powerTable[0].PubKey,
		}})
		require.ErrorContains(t, err, "includes an unchanged key")

		// Empty power change
		_, err = certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(0),
		}})
		require.ErrorContains(t, err, "empty delta")

		// Remove all power and change key.
		_, err = certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    powerTable[0].Power.Neg(),
			SigningKey:    powerTable[1].PubKey,
		}})
		require.ErrorContains(t, err, "removes all power for participant 1 while specifying a new key")

		// Add an entry with a non-positive power.
		_, err = certs.ApplyPowerTableDiffs(nil, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(0),
			SigningKey:    powerTable[0].PubKey,
		}})
		require.ErrorContains(t, err, "new entry with a non-positive power delta")

		// Add an entry with a negative power.
		_, err = certs.ApplyPowerTableDiffs(nil, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(-1),
			SigningKey:    powerTable[0].PubKey,
		}})
		require.ErrorContains(t, err, "new entry with a non-positive power delta")

		// Add an entry with no key.
		_, err = certs.ApplyPowerTableDiffs(nil, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    gpbft.NewStoragePower(1),
		}})
		require.ErrorContains(t, err, "empty signing key")

		// Remove more power than we have.
		_, err = certs.ApplyPowerTableDiffs(powerTable, certs.PowerTableDiff{{
			ParticipantID: powerTable[0].ID,
			PowerDelta:    big.Sub(gpbft.NewStoragePower(-1), powerTable[0].Power),
		}})
		require.ErrorContains(t, err, "resulted in negative power")
	}

}

func testRoundTrip(t *testing.T, diff certs.PowerTableDiff) {
	var b bytes.Buffer
	require.NoError(t, diff.MarshalCBOR(&b))

	var out certs.PowerTableDiff
	require.NoError(t, out.UnmarshalCBOR(&b))

	require.EqualValues(t, diff, out)
}

func TestFinalityCertificates(t *testing.T) {
	backend := signing.NewFakeBackend()

	powerTable := randomPowerTable(backend, 100)
	maxPower := int64(len(powerTable) * 2)
	tableCid, err := certs.MakePowerTableCID(powerTable)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(1234))
	tsg := sim.NewTipSetGenerator(rng.Uint64())
	base := gpbft.TipSet{Epoch: 0, Key: tsg.Sample(), PowerTable: tableCid}

	certificates := make([]certs.FinalityCertificate, 10)
	powerTables := make([]gpbft.PowerEntries, 10)
	var removedPowerEntries []gpbft.PowerEntry
	for i := range certificates {
		powerTables[i] = powerTable
		powerTable, removedPowerEntries = randomizePowerTable(rng, backend, maxPower, powerTable, removedPowerEntries)
		justification := makeJustification(t, rng, tsg, backend, base, uint64(i), powerTables[i], powerTable)
		cert, err := certs.NewFinalityCertificate(certs.MakePowerTableDiff(powerTables[i], powerTable), justification)
		require.NoError(t, err)
		certificates[i] = *cert
		base = *justification.Vote.Value.Head()
	}

	// Validate one.
	nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTables[0], 0, certificates[0].ECChain.Base(), certificates[0])
	require.NoError(t, err)
	require.EqualValues(t, 1, nextInstance)
	require.True(t, chain.Eq(certificates[0].ECChain.Suffix()))
	require.Equal(t, powerTables[1], newPowerTable)

	// Validate multiple
	nextInstance, chain, newPowerTable, err = certs.ValidateFinalityCertificates(backend, networkName, powerTables[0], 0, nil, certificates[:4]...)
	require.NoError(t, err)
	require.EqualValues(t, 4, nextInstance)
	require.Equal(t, powerTables[4], newPowerTable)
	require.True(t, certificates[3].ECChain.Head().Equal(chain.Head()))
	require.True(t, certificates[0].ECChain[1].Equal(chain.Base()))

	nextInstance, chain, newPowerTable, err = certs.ValidateFinalityCertificates(backend, networkName, powerTables[nextInstance], nextInstance, nil, certificates[nextInstance:]...)
	require.NoError(t, err)
	require.EqualValues(t, len(certificates), nextInstance)
	require.Equal(t, powerTable, newPowerTable)
	require.True(t, certificates[len(certificates)-1].ECChain.Head().Equal(chain.Head()))
	require.True(t, certificates[4].ECChain[1].Equal(chain.Base()))
}

func TestBadFinalityCertificates(t *testing.T) {
	backend := signing.NewFakeBackend()
	powerTable := randomPowerTable(backend, 100)
	rng := rand.New(rand.NewSource(1234))
	tsg := sim.NewTipSetGenerator(rng.Uint64())
	tableCid, err := certs.MakePowerTableCID(powerTable)
	require.NoError(t, err)
	base := gpbft.TipSet{Epoch: 0, Key: tsg.Sample(), PowerTable: tableCid}

	nextPowerTable, _ := randomizePowerTable(rng, backend, 200, powerTable, nil)

	justification := makeJustification(t, rng, tsg, backend, base, 1, powerTable, nextPowerTable)

	// Alter the phase
	{
		jCopy := *justification
		jCopy.Vote.Phase = gpbft.COMMIT_PHASE
		_, err = certs.NewFinalityCertificate(nil, &jCopy)
		require.ErrorContains(t, err, "can only create a finality certificate from a decide vote")
	}
	// Alter the round (must be 0)
	{
		jCopy := *justification
		jCopy.Vote.Round = 1
		_, err = certs.NewFinalityCertificate(nil, &jCopy)
		require.ErrorContains(t, err, "decide round to be 0, got round 1")
	}
	// Empty value.
	{
		jCopy := *justification
		jCopy.Vote.Value = nil
		_, err = certs.NewFinalityCertificate(nil, &jCopy)
		require.ErrorContains(t, err, "got a decision for bottom")
	}

	powerDiff := certs.MakePowerTableDiff(powerTable, nextPowerTable)
	certificate, err := certs.NewFinalityCertificate(powerDiff, justification)
	require.NoError(t, err)

	// Unexpected instance number
	{
		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable, 0, nil, *certificate)
		require.ErrorContains(t, err, "expected instance 0, found instance 1")
		require.EqualValues(t, 0, nextInstance)
		require.Equal(t, powerTable, newPowerTable)
		require.Empty(t, chain)
	}
	// Wrong base.
	{
		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable, 1, certificate.ECChain.Head(), *certificate)
		require.ErrorContains(t, err, "base tipset does not match finalized chain")
		require.EqualValues(t, 1, nextInstance)
		require.Equal(t, powerTable, newPowerTable)
		require.Empty(t, chain)
	}

	// Discard most of the power table. Given the initial power distribution, we can guarantee
	// that we require more than 10 participants.
	{
		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable[:10], 1, nil, *certificate)
		require.ErrorContains(t, err, "but we only have 10 entries in the power table")
		require.EqualValues(t, 1, nextInstance)
		require.Equal(t, powerTable[:10], newPowerTable)
		require.Empty(t, chain)
	}

	// Swap out the first signer's key
	{
		firstSigner, err := certificate.Signers.First()
		require.NoError(t, err)
		powerTableCpy := slices.Clone(powerTable)
		powerTableCpy[firstSigner].PubKey = powerTableCpy[(int(firstSigner)+1)%len(powerTableCpy)].PubKey
		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, "invalid signature on finality certificate")
		require.EqualValues(t, 1, nextInstance)
		require.Empty(t, chain)
	}

	// Mutate the power such that the delta doesn't apply.
	{
		firstSigner, err := certificate.Signers.First()
		require.NoError(t, err)
		powerTableCpy := slices.Clone(powerTable)
		// increase so we definitely have enough power
		powerTableCpy[firstSigner].Power = big.Add(powerTableCpy[firstSigner].Power, gpbft.NewStoragePower(1))
		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, "incorrect power diff")
		require.EqualValues(t, 1, nextInstance)
		require.Empty(t, chain)
	}

	// Give one signer enough power such that all others round to zero.
	{
		powerTableCpy := slices.Clone(powerTable)
		for i := range powerTableCpy {
			powerTableCpy[i].Power = gpbft.NewStoragePower(1)
		}

		firstSigner, err := certificate.Signers.First()
		require.NoError(t, err)
		powerTableCpy[firstSigner].Power = gpbft.NewStoragePower(0xffff)

		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, "no effective power after scaling")
		require.EqualValues(t, 1, nextInstance)
		require.Empty(t, chain)
	}

	// Reduce active power to 1, can't have quorum.
	{
		powerTableCpy := slices.Clone(powerTable)
		require.NoError(t, certificate.Signers.ForEach(func(i uint64) error {
			powerTableCpy[i].Power = gpbft.NewStoragePower(1)
			return nil
		}))
		scaledPowerTable, totalPower, err := powerTableCpy.Scaled()
		require.NoError(t, err)
		var activePower int64
		require.NoError(t, certificate.Signers.ForEach(func(i uint64) error {
			activePower += scaledPowerTable[i]
			return nil
		}))

		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, fmt.Sprintf("has insufficient power: %d < 2/3 %d", activePower, totalPower))
		require.EqualValues(t, 1, nextInstance)
		require.Empty(t, chain)
	}

	// Chain is bottom.
	{
		certCpy := *certificate
		certCpy.ECChain = nil
		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable, 1, nil, certCpy)
		require.ErrorContains(t, err, "empty finality certificate")
		require.EqualValues(t, 1, nextInstance)
		require.Equal(t, powerTable, newPowerTable)
		require.Empty(t, chain)
	}

	// Chain is invalid.
	{
		certCpy := *certificate
		certCpy.ECChain = slices.Clone(certCpy.ECChain)
		slices.Reverse(certCpy.ECChain)
		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable, 1, nil, certCpy)
		require.ErrorContains(t, err, "chain must have increasing epochs")
		require.EqualValues(t, 1, nextInstance)
		require.Equal(t, powerTable, newPowerTable)
		require.Empty(t, chain)
	}

	// Power table diff is invalid (already tested in ApplyPowerTableDiffs tests, but this makes
	// sure we don't try something fancy here).
	{
		certCpy := *certificate
		certCpy.PowerTableDelta = slices.Clone(certCpy.PowerTableDelta)
		// empty diff is invalid.
		certCpy.PowerTableDelta[0].PowerDelta = gpbft.NewStoragePower(0)
		certCpy.PowerTableDelta[0].SigningKey = nil

		nextInstance, chain, newPowerTable, err := certs.ValidateFinalityCertificates(backend, networkName, powerTable, 1, nil, certCpy)
		require.ErrorContains(t, err, "failed to apply power table delta")
		require.EqualValues(t, 1, nextInstance)
		require.Equal(t, powerTable, newPowerTable)
		require.Empty(t, chain)
	}
}

func randomizePowerTable(rng *rand.Rand, backend signing.Backend, maxPower int64, livePowerEntries, deadPowerEntries gpbft.PowerEntries) (_livePowerEntries, _deadPowerEntries gpbft.PowerEntries) {
	const (
		Power int = iota
		Key
		Add
		Remove
		NumOps
	)
	livePowerEntries = slices.Clone(livePowerEntries)
	deadPowerEntries = slices.Clone(deadPowerEntries)
	for j := 0; j < rng.Intn(10); j++ {
		switch rng.Intn(NumOps) {
		case Power:
			k := rng.Intn(len(livePowerEntries))
			livePowerEntries[k].Power = gpbft.NewStoragePower(rng.Int63n(maxPower-1) + 1)
		case Key:
			k := rng.Intn(len(livePowerEntries))
			livePowerEntries[k].PubKey, _ = backend.GenerateKey()
		case Add:
			if len(deadPowerEntries) == 0 {
				continue
			}
			k := rng.Intn(len(deadPowerEntries))
			livePowerEntries = append(livePowerEntries, deadPowerEntries[k])
			deadPowerEntries[k] = deadPowerEntries[len(deadPowerEntries)-1]
			deadPowerEntries = deadPowerEntries[:len(deadPowerEntries)-1]
		case Remove:
			if len(livePowerEntries) == 1 {
				continue
			}
			k := rng.Intn(len(livePowerEntries))
			deadPowerEntries = append(deadPowerEntries, livePowerEntries[k])
			livePowerEntries[k] = livePowerEntries[len(livePowerEntries)-1]
			livePowerEntries = livePowerEntries[:len(livePowerEntries)-1]
		}
	}
	sort.Sort(livePowerEntries)
	return livePowerEntries, deadPowerEntries
}

func randomPowerTable(backend signing.Backend, entries int64) gpbft.PowerEntries {
	powerTable := make(gpbft.PowerEntries, entries)

	for i := range powerTable {
		key, _ := backend.GenerateKey()
		powerTable[i] = gpbft.PowerEntry{
			ID: gpbft.ActorID(i + 1),
			// Power chosen such that:
			// - No small subset dominates the power table.
			// - Lots of duplicate power values.
			Power:  gpbft.NewStoragePower(int64(len(powerTable)*2 - i/2)),
			PubKey: key,
		}
	}
	return powerTable
}

func makeJustification(t *testing.T, rng *rand.Rand, tsg *sim.TipSetGenerator, backend signing.Backend, base gpbft.TipSet, instance uint64, powerTable, nextPowerTable gpbft.PowerEntries) *gpbft.Justification {
	chainLen := rng.Intn(23) + 1
	chain, err := gpbft.NewChain(base)
	require.NoError(t, err)

	for i := 0; i < chainLen; i++ {
		chain = chain.Extend(tsg.Sample())
	}

	j, err := sim.MakeJustification(backend, networkName, chain, instance, powerTable, nextPowerTable)
	require.NoError(t, err)

	return j
}
