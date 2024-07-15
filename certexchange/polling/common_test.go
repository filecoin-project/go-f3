package polling

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"

	"github.com/stretchr/testify/require"
)

// The network name used in tests.
const TestNetworkName gpbft.NetworkName = "testnet"

func MakeCertificate(t *testing.T, rng *rand.Rand, tsg *sim.TipSetGenerator, backend signing.Backend, base *gpbft.TipSet, instance uint64, powerTable, nextPowerTable gpbft.PowerEntries) *certs.FinalityCertificate {
	chainLen := rng.Intn(23) + 1
	chain, err := gpbft.NewChain(*base)
	require.NoError(t, err)

	for i := 0; i < chainLen; i++ {
		chain = chain.Extend(tsg.Sample())
	}

	j, err := sim.MakeJustification(backend, TestNetworkName, chain, instance, powerTable, nextPowerTable)
	require.NoError(t, err)

	c, err := certs.NewFinalityCertificate(certs.MakePowerTableDiff(powerTable, nextPowerTable), j)
	require.NoError(t, err)

	return c
}

func RandomPowerTable(backend signing.Backend, entries int64) gpbft.PowerEntries {
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

func MakeCertificates(t *testing.T, rng *rand.Rand, backend signing.Backend) *CertificateGenerator {
	powerTable := RandomPowerTable(backend, 10)
	tableCid, err := certs.MakePowerTableCID(powerTable)
	require.NoError(t, err)

	tsg := sim.NewTipSetGenerator(rng.Uint64())
	base := &gpbft.TipSet{Epoch: 0, Key: tsg.Sample(), PowerTable: tableCid}

	return &CertificateGenerator{
		PowerTable:   powerTable,
		t:            t,
		rng:          rng,
		backend:      backend,
		tsg:          tsg,
		base:         base,
		NextInstance: 0,
	}
}

type CertificateGenerator struct {
	PowerTable   gpbft.PowerEntries
	NextInstance uint64

	t       *testing.T
	rng     *rand.Rand
	backend signing.Backend
	tsg     *sim.TipSetGenerator
	base    *gpbft.TipSet
}

func (cg *CertificateGenerator) MakeCertificate() *certs.FinalityCertificate {
	cert := MakeCertificate(cg.t, cg.rng, cg.tsg, cg.backend, cg.base, cg.NextInstance, cg.PowerTable, cg.PowerTable)
	cg.base = cert.ECChain.Head()
	cg.NextInstance++
	return cert
}
