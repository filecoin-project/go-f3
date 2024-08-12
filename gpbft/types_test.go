package gpbft_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func pubKeyFrom[V string | []byte](v V) gpbft.PubKey {
	var subject gpbft.PubKey
	copy(subject[:], v)
	return subject
}

func TestPubKey_IsZero(t *testing.T) {
	var subject gpbft.PubKey
	require.True(t, subject.IsZero())
	require.False(t, pubKeyFrom("fish").IsZero())
}
