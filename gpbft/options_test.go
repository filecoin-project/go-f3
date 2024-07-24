package gpbft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_exponentialBackoffer(t *testing.T) {
	maxBackoff := 30 * time.Second
	backoffer := exponentialBackoffer(1.3, 3*time.Second, maxBackoff)
	for i := 0; i < 10_000; i++ {
		backoff := backoffer(i)
		require.Positivef(t, backoff, "at %d", i)
		require.LessOrEqualf(t, backoff, maxBackoff, "at %d", i)
	}
}
