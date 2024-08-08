package blssig

import (
	"runtime"
	"testing"

	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/sign/bdn"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-f3/gpbft"
)

const maxCacheMemory uint64 = 10 * 1023 * 1024 // 10MiB

func TestCacheMemory(t *testing.T) {
	suite := bls12381.NewBLS12381Suite()
	scheme := bdn.NewSchemeOnG2(suite)

	rand := suite.RandomStream()
	keys := make([]gpbft.PubKey, maxPointCacheSize+1)
	for i := range keys {
		_, pub := scheme.NewKeyPair(rand)
		pubKeyB, err := pub.MarshalBinary()
		require.NoError(t, err)
		keys[i] = pubKeyB
	}

	runtime.GC()
	var beforeMemStats, afterMemStats runtime.MemStats
	runtime.ReadMemStats(&beforeMemStats)
	v := VerifierWithKeyOnG1()
	for _, k := range keys[1:] {
		_, err := v.pubkeyToPoint(k)
		require.NoError(t, err)
	}
	runtime.ReadMemStats(&afterMemStats)
	require.Less(t, afterMemStats.HeapAlloc-beforeMemStats.HeapAlloc, maxCacheMemory)

	require.Len(t, v.pointCache, maxPointCacheSize)

	_, err := v.pubkeyToPoint(keys[1])
	require.NoError(t, err)

	require.Len(t, v.pointCache, maxPointCacheSize)

	_, err = v.pubkeyToPoint(keys[0])
	require.NoError(t, err)

	require.Len(t, v.pointCache, 1)
}
