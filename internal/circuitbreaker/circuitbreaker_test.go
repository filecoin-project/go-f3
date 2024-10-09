package circuitbreaker_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/internal/circuitbreaker"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()

	const (
		maxFailures = 3
		restTimeout = 10 * time.Millisecond

		eventualTimeout = restTimeout * 2
		eventualTick    = restTimeout / 5
	)

	var (
		failure = errors.New("fish out of water")

		succeed = func() error { return nil }
		fail    = func() error { return failure }
		trip    = func(t *testing.T, subject *circuitbreaker.CircuitBreaker) {
			for range maxFailures {
				require.ErrorContains(t, subject.Run(fail), "fish")
			}
			require.Equal(t, circuitbreaker.Open, subject.GetStatus())
		}
	)

	t.Run("closed on no error", func(t *testing.T) {
		t.Parallel()
		subject := circuitbreaker.New(maxFailures, restTimeout)
		require.NoError(t, subject.Run(succeed))
		require.Equal(t, circuitbreaker.Closed, subject.GetStatus())
	})

	t.Run("opens after max failures and stays open", func(t *testing.T) {
		subject := circuitbreaker.New(maxFailures, restTimeout)
		trip(t, subject)

		// Assert that immediate runs fail, without being attempted, even if they would
		// be successful until restTimeout has elapsed.
		err := subject.Run(succeed)
		require.ErrorIs(t, err, circuitbreaker.ErrOpen)
		require.Equal(t, circuitbreaker.Open, subject.GetStatus())
	})

	t.Run("half-opens eventually", func(t *testing.T) {
		subject := circuitbreaker.New(maxFailures, restTimeout)
		trip(t, subject)
		require.ErrorIs(t, subject.Run(fail), circuitbreaker.ErrOpen)
		// Assert that given function is eventually run after circuit is tripped at
		// half-open status by checking error type.
		require.Eventually(t, func() bool { return errors.Is(subject.Run(fail), failure) }, eventualTimeout, eventualTick)
	})

	t.Run("closes after rest timeout and success", func(t *testing.T) {
		subject := circuitbreaker.New(maxFailures, restTimeout)
		trip(t, subject)

		require.Eventually(t, func() bool { return subject.Run(succeed) == nil }, eventualTimeout, eventualTick)
		require.Equal(t, circuitbreaker.Closed, subject.GetStatus())
	})

	t.Run("usable concurrently", func(t *testing.T) {
		subject := circuitbreaker.New(maxFailures, restTimeout)
		const (
			wantSuccesses = 7
			totalAttempts = 1_000
		)
		var (
			successes, failures int
			wg                  sync.WaitGroup
		)
		for range totalAttempts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = subject.Run(func() error {
					// Unsafely increment/decrement counters so that if Run is not synchronised
					// properly the test creates a race condition.
					if successes < wantSuccesses {
						successes++
						return nil
					}
					failures++
					return errors.New("error")
				})
			}()
		}
		wg.Wait()
		require.Equal(t, wantSuccesses, successes)
		require.Equal(t, maxFailures, failures)
	})
}
