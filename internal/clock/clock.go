package clock

import (
	"context"

	"github.com/benbjohnson/clock"
)

type Clock = clock.Clock
type Mock = clock.Mock
type Timer = clock.Timer

type clockKeyType struct{}

var clockKey = clockKeyType{}

// NewMock returns an instance of a mock clock.
// The current time of the mock clock on initialization is the Unix epoch.
func NewMock() *Mock {
	return clock.NewMock()
}

// WithMockClock embeds a mock clock in the context and returns it.
func WithMockClock(ctx context.Context) (context.Context, *Mock) {
	clk := clock.NewMock()
	return context.WithValue(ctx, clockKey, (Clock)(clk)), clk
}

var realClock = clock.New()

// GetClock either retrieves a mock clock from the context or returns a realtime clock.
func GetClock(ctx context.Context) Clock {
	clk := ctx.Value(clockKey)
	if clk == nil {
		return realClock
	}
	return clk.(Clock)
}
