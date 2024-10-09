package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	Closed Status = iota
	Open
	HalfOpen
)

// ErrOpen signals that the circuit is open. See CircuitBreaker.Run.
var ErrOpen = errors.New("circuit breaker is open")

type Status int

type CircuitBreaker struct {
	maxFailures  int
	resetTimeout time.Duration

	// mu guards access to status, lastFailure and failures.
	mu          sync.Mutex
	failures    int
	lastFailure time.Time
	status      Status
}

// New creates a new CircuitBreaker instance with the specified maximum number
// of failures and a reset timeout duration.
//
// See CircuitBreaker.Run.
func New(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
	}
}

// Run attempts to execute the provided function within the context of the
// circuit breaker. It handles state transitions, Closed, Open, or HalfOpen,
// based on the outcome of the attempt.
//
// If the circuit is in the Open state, and not enough time has passed since the
// last failure, the circuit remains open, and the function returns
// ErrOpen without attempting the provided function. If enough time
// has passed, the circuit transitions to HalfOpen, and one attempt is allowed.
//
// In HalfOpen state if the function is executed and returns an error, the
// circuit breaker will transition back to Open status. Otherwise, if the
// function executes successfully, the circuit resets to the Closed state, and
// the failure count is reset to zero.
//
// Example:
//
//	cb := NewCircuitBreaker(3, time.Second)
//	switch err := cb.Run(func() error {
//		// Your attempt logic here
//		return nil
//	}); {
//	case errors.Is(err, ErrCircuitBreakerOpen):
//		// No execution attempt was made since the circuit is open.
//	case err != nil:
//		// Execution attempt failed.
//	default:
//		// Execution attempt succeeded.
//	}
func (cb *CircuitBreaker) Run(attempt func() error) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.status {
	case Open:
		if time.Since(cb.lastFailure) < cb.resetTimeout {
			// Not enough time has passed since the circuit opened. Do not make any further
			// attempts.
			return ErrOpen
		}
		// Enough time has passed since last failure. Proceed to allow one attempt by
		// half-opening the circuit.
		cb.status = HalfOpen
		fallthrough
	case HalfOpen, Closed:
		if err := attempt(); err != nil {
			cb.failures++
			if cb.failures >= cb.maxFailures {
				// Trip the circuit as we are at or above the max failure threshold.
				cb.status = Open
				cb.lastFailure = time.Now()
			}
			return err
		}
		// Reset the circuit since the attempt succeeded.
		cb.status = Closed
		cb.failures = 0
		return nil
	default:
		return fmt.Errorf("unknown status: %d", cb.status)
	}
}

// GetStatus returns the current status of the CircuitBreaker.
func (cb *CircuitBreaker) GetStatus() Status {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.status
}
