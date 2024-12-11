package f3

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/certchain"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestCompression_CertificateChain(t *testing.T) {

	const (
		seed                   = 1427
		certChainLength        = 1_00
		powerTableSize         = 2000
		maxPower               = 1 << 20
		minPower               = 1 << 10
		actorIDOffset          = 1413
		powerChangeProbability = 0.01
	)

	var powerTableCache = make(map[int64]gpbft.PowerEntries)

	generatePowerTable := func(t *testing.T, rng *rand.Rand, generatePublicKey func(id gpbft.ActorID) gpbft.PubKey, previousEntries gpbft.PowerEntries) gpbft.PowerEntries {

		size := powerTableSize
		entries := make(gpbft.PowerEntries, 0, size)
		for i := range size {
			var entry gpbft.PowerEntry
			if i < previousEntries.Len() {
				entry = previousEntries[i]
				changedPower := rng.Float64() > powerChangeProbability
				if changedPower {
					entry.Power = gpbft.NewStoragePower(int64(rng.Intn(maxPower) + minPower))
				}
			} else {
				id := gpbft.ActorID(uint64(actorIDOffset + i))
				entry = gpbft.PowerEntry{
					ID:     id,
					Power:  gpbft.NewStoragePower(int64(rng.Intn(maxPower) + minPower)),
					PubKey: generatePublicKey(id),
				}
			}
			entries = append(entries, entry)
		}
		next := gpbft.NewPowerTable()
		require.NoError(t, next.Add(entries...))
		return next.Entries
	}

	ctx, clk := clock.WithMockClock(context.Background())
	m := manifest.LocalDevnetManifest()
	signVerifier := signing.NewFakeBackend()
	rng := rand.New(rand.NewSource(seed * 23))
	generatePublicKey := func(id gpbft.ActorID) gpbft.PubKey {
		//TODO: add the ability to evolve public key across instances. Fake signing
		//      backed does not support this.

		// Use allow instead of GenerateKey for a reproducible key generation.
		return signVerifier.Allow(int(id))
	}
	initialPowerTable := generatePowerTable(t, rng, generatePublicKey, nil)

	ec := consensus.NewFakeEC(ctx,
		consensus.WithSeed(seed*13),
		consensus.WithBootstrapEpoch(m.BootstrapEpoch),
		consensus.WithECPeriod(m.EC.Period),
		consensus.WithInitialPowerTable(initialPowerTable),
		consensus.WithEvolvingPowerTable(
			func(epoch int64, entries gpbft.PowerEntries) gpbft.PowerEntries {
				if epoch == m.BootstrapEpoch-m.EC.Finality {
					return initialPowerTable
				}
				if cachedEntries, exists := powerTableCache[epoch]; exists {
					return cachedEntries
				}
				rng := rand.New(rand.NewSource(epoch * seed))
				next := generatePowerTable(t, rng, generatePublicKey, entries)
				powerTableCache[epoch] = next
				return next
			},
		),
	)

	subject, err := certchain.New(
		certchain.WithSeed(seed),
		certchain.WithSignVerifier(signVerifier),
		certchain.WithManifest(m),
		certchain.WithEC(ec),
	)
	require.NoError(t, err)

	clk.Add(200 * time.Hour)

	generatedChain, err := subject.Generate(ctx, certChainLength)
	require.NoError(t, err)

	var uncompressed, compressed bytes.Buffer
	minGain, maxGain, totalGain := math.MaxFloat64, 0.0, 0.0

	for _, certificate := range generatedChain {
		require.NoError(t, certificate.MarshalCBOR(&uncompressed))
		compressor, err := zstd.NewWriter(&compressed)
		require.NoError(t, err)
		_, err = compressor.Write(uncompressed.Bytes())
		require.NoError(t, err)
		require.NoError(t, compressor.Flush())

		compression := float64(uncompressed.Len()-compressed.Len()) / float64(uncompressed.Len())
		minGain = min(minGain, compression)
		maxGain = max(maxGain, compression)
		totalGain += compression

		uncompressed.Reset()
		compressed.Reset()
	}

	t.Logf("Min compression gain: %.1f %%\n", minGain*100)
	t.Logf("Max compression gain: %.1f %%\n", maxGain*100)
	t.Logf("Avg compression gain: %.1f %%\n", totalGain/float64(len(generatedChain))*100)
}
