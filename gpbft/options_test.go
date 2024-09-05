package gpbft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_exponentialBackofferMax(t *testing.T) {
	maxBackoff := 30 * time.Second
	backoffer := exponentialBackoffer(1.3, 0, 3*time.Second, maxBackoff)
	var lastBackoff time.Duration
	for i := 0; i < 10_000; i++ {
		backoff := backoffer(i)
		require.Positivef(t, backoff, "at %d", i)
		if backoff != maxBackoff {
			require.Less(t, backoff, maxBackoff, "at %d", i)
			require.Greater(t, backoff, lastBackoff, "at %d", i)
		}
	}
}

func Test_exponentialBackofferSpread(t *testing.T) {
	maxBackoff := 30 * time.Second
	backoffer1 := exponentialBackoffer(1.3, 0.1, 3*time.Second, maxBackoff)
	backoffer2 := exponentialBackoffer(1.3, 0.1, 3*time.Second, maxBackoff)

	for i := 0; i < 8; i++ {
		backoff1 := backoffer1(i)
		backoff2 := backoffer2(i)
		require.NotEqual(t, backoff1, backoff2, "backoffs were not randomized")
	}
}
