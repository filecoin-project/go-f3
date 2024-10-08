package blssig

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/kyber/v4/sign/bdn"

	"github.com/filecoin-project/go-f3/gpbft"
	bls12381 "github.com/filecoin-project/go-f3/internal/gnark"
)

const maxCacheMemory uint64 = 10 << 20 // 10MiB

func TestCacheMemory(t *testing.T) {
	suite := bls12381.NewSuiteBLS12381()
	scheme := bdn.NewSchemeOnG2(suite)

	rand := suite.RandomStream()
	keys := make([]gpbft.PubKey, maxPointCacheSize+1)
	for i := range keys {
		_, pub := scheme.NewKeyPair(rand)
		pubKeyB, err := pub.MarshalBinary()
		require.NoError(t, err)
		require.Len(t, pubKeyB, 48)
		keys[i] = pubKeyB
	}

	runtime.GC()
	runtime.GC()
	var beforeMemStats, afterMemStats runtime.MemStats
	runtime.ReadMemStats(&beforeMemStats)
	v := VerifierWithKeyOnG1()
	for _, k := range keys[1:] {
		_, err := v.pubkeyToPoint(k)
		require.NoError(t, err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&afterMemStats)
	memUse := afterMemStats.HeapAlloc - beforeMemStats.HeapAlloc
	t.Log(memUse)
	require.Less(t, memUse, maxCacheMemory)

	require.Len(t, v.pointCache, maxPointCacheSize)

	_, err := v.pubkeyToPoint(keys[1])
	require.NoError(t, err)

	require.Len(t, v.pointCache, maxPointCacheSize)

	_, err = v.pubkeyToPoint(keys[0])
	require.NoError(t, err)

	require.Len(t, v.pointCache, 1)
}
