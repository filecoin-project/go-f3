package polling_test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/signing"

	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

const testNetworkName gpbft.NetworkName = "testnet"

var log = logging.Logger("certexchange-poller-test")

func init() {
	logging.SetLogLevel("certexchange-poller-test", "debug")
}

func makeCertificate(t *testing.T, rng *rand.Rand, tsg *sim.TipSetGenerator, backend signing.Backend, base *gpbft.TipSet, instance uint64, powerTable, nextPowerTable gpbft.PowerEntries) *certs.FinalityCertificate {
	chainLen := rng.Intn(23) + 1
	chain, err := gpbft.NewChain(*base)
	require.NoError(t, err)

	for i := 0; i < chainLen; i++ {
		chain = chain.Extend(tsg.Sample())
	}

	j, err := sim.MakeJustification(backend, testNetworkName, chain, instance, powerTable, nextPowerTable)
	require.NoError(t, err)

	c, err := certs.NewFinalityCertificate(certs.MakePowerTableDiff(powerTable, nextPowerTable), j)
	require.NoError(t, err)

	return c
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

func makeCertificates(t *testing.T, rng *rand.Rand, backend signing.Backend) ([]*certs.FinalityCertificate, gpbft.PowerEntries) {
	powerTable := randomPowerTable(backend, 10)
	tableCid, err := certs.MakePowerTableCID(powerTable)
	require.NoError(t, err)

	tsg := sim.NewTipSetGenerator(rng.Uint64())
	base := &gpbft.TipSet{Epoch: 0, Key: tsg.Sample(), PowerTable: tableCid}

	certificates := make([]*certs.FinalityCertificate, 1000)
	for i := range certificates {
		cert := makeCertificate(t, rng, tsg, backend, base, uint64(i), powerTable, powerTable)
		base = cert.ECChain.Head()
		certificates[i] = cert
	}
	return certificates, powerTable
}
