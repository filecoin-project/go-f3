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

func NewMock() *Mock {
	return clock.NewMock()
}

func WithMockClock(ctx context.Context) (context.Context, *Mock) {
	clk := clock.NewMock()
	return context.WithValue(ctx, clockKey, (Clock)(clk)), clk
}

var realClock = clock.New()

func GetClock(ctx context.Context) Clock {
	clk := ctx.Value(clockKey)
	if clk == nil {
		return realClock
	}
	return clk.(Clock)
}
