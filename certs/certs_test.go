package certs_test

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

const networkName = "f3test"

func makePowerTableCID(pt gpbft.PowerEntries) (gpbft.CID, error) {
	var buf bytes.Buffer
	if err := pt.MarshalCBOR(&buf); err != nil {
		return nil, xerrors.Errorf("failed to serialize power table: %w", err)
	}
	return gpbft.MakeCid(buf.Bytes()), nil
}

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
		expDeltaRemove := make([]certs.PowerTableDelta, len(powerTable))
		expDeltaAdd := make([]certs.PowerTableDelta, len(powerTable))
		for i, e := range powerTable {
			expDeltaAdd[i] = certs.PowerTableDelta{
				ParticipantID: e.ID,
				PowerDelta:    e.Power,
				SigningKey:    e.PubKey,
			}

			expDeltaRemove[i] = certs.PowerTableDelta{
				ParticipantID: e.ID,
				PowerDelta:    new(gpbft.StoragePower).Neg(e.Power),
			}

		}
		require.Equal(t, expDeltaRemove, certs.MakePowerTableDiff(powerTable, nil))
		require.Equal(t, expDeltaAdd, certs.MakePowerTableDiff(nil, powerTable))

		addAll, err := certs.ApplyPowerTableDiffs(nil, expDeltaAdd)
		require.NoError(t, err)
		require.Equal(t, powerTable, addAll)
		remAll, err := certs.ApplyPowerTableDiffs(powerTable, expDeltaRemove)
		require.NoError(t, err)
		require.Empty(t, remAll)
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
}

func TestFinalityCertificates(t *testing.T) {
	backend := signing.NewFakeBackend()

	powerTable := randomPowerTable(backend, 100)
	maxPower := int64(len(powerTable) * 2)
	tableCid, err := makePowerTableCID(powerTable)
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
	tableCid, err := makePowerTableCID(powerTable)
	require.NoError(t, err)
	base := gpbft.TipSet{Epoch: 0, Key: tsg.Sample(), PowerTable: tableCid}

	justification := makeJustification(t, rng, tsg, backend, base, 1, powerTable, powerTable)

	// Alter the step
	{
		jCopy := *justification
		jCopy.Vote.Step = gpbft.COMMIT_PHASE
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

	certificate, err := certs.NewFinalityCertificate(nil, justification)
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
		powerTableCpy[firstSigner].Power = new(gpbft.StoragePower).Add(powerTableCpy[firstSigner].Power, gpbft.NewStoragePower(1))
		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, "incorrect power diff")
		require.EqualValues(t, 1, nextInstance)
		require.Empty(t, chain)
	}

	// Reduce active power to 1, can't have quorum.
	{
		powerTableCpy := slices.Clone(powerTable)
		count := 0
		require.NoError(t, certificate.Signers.ForEach(func(i uint64) error {
			powerTableCpy[i].Power = gpbft.NewStoragePower(1)
			count++
			return nil
		}))
		nextInstance, chain, _, err := certs.ValidateFinalityCertificates(backend, networkName, powerTableCpy, 1, nil, *certificate)
		require.ErrorContains(t, err, fmt.Sprintf("has insufficient power: %d", count))
		require.EqualValues(t, 1, nextInstance)
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

	strongThreshold := new(gpbft.StoragePower)
	for _, pe := range powerTable {
		strongThreshold = strongThreshold.Add(strongThreshold, pe.Power)
	}

	strongThreshold = strongThreshold.Mul(strongThreshold, gpbft.NewStoragePower(2))
	strongThreshold = strongThreshold.Div(strongThreshold, gpbft.NewStoragePower(3))

	powerTableCid, err := makePowerTableCID(nextPowerTable)
	require.NoError(t, err)

	payload := gpbft.Payload{
		Instance: instance,
		Round:    0,
		Step:     gpbft.DECIDE_PHASE,
		SupplementalData: gpbft.SupplementalData{
			PowerTable: powerTableCid,
		},
		Value: chain,
	}
	msg := backend.MarshalPayloadForSigning(networkName, &payload)
	signers := rand.Perm(len(powerTable))
	signersBitfield := bitfield.New()
	signingPower := new(gpbft.StoragePower)

	type vote struct {
		index int
		sig   []byte
		pk    gpbft.PubKey
	}

	var votes []vote
	for _, i := range signers {
		pe := powerTable[i]
		sig, err := backend.Sign(pe.PubKey, msg)
		require.NoError(t, err)
		votes = append(votes, vote{
			index: i,
			sig:   sig,
			pk:    pe.PubKey,
		})

		signersBitfield.Set(uint64(i))
		signingPower = signingPower.Add(signingPower, pe.Power)
		if signingPower.Cmp(strongThreshold) > 0 {
			break
		}
	}
	slices.SortFunc(votes, func(a, b vote) int {
		return cmp.Compare(a.index, b.index)
	})
	pks := make([]gpbft.PubKey, len(votes))
	sigs := make([][]byte, len(votes))
	for i, vote := range votes {
		pks[i] = vote.pk
		sigs[i] = vote.sig
	}

	sig, err := backend.Aggregate(pks, sigs)
	require.NoError(t, err)

	return &gpbft.Justification{
		Vote:      payload,
		Signers:   signersBitfield,
		Signature: sig,
	}
}
