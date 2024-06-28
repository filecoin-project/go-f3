package emulator

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ gpbft.Host = (*mockHost)(nil)

// Mock implementation of gpbft.Host for use in tests.
type mockHost struct {
	chain
	nilSigning
	now          time.Time
	pendingAlarm *time.Time
	received     []*gpbft.MessageBuilder
}

func newHost() *mockHost {
	return &mockHost{
		chain: newChain(),
	}
}

func (h *mockHost) NetworkName() gpbft.NetworkName { return "emulator-net" }
func (h *mockHost) Time() time.Time                { return h.now }
func (h *mockHost) SetAlarm(at time.Time)          { h.pendingAlarm = &at }

func (h *mockHost) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	h.received = append(h.received, mb)
	return nil
}

func (h *mockHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	if err := h.setDecision(decision); err != nil {
		// TODO: add the ability to set callback and make emulated behavior here configurable.
		panic(err)
	}
	return h.now
}

func (h *mockHost) maybeConsumeAlarm() bool {
	if h.pendingAlarm == nil {
		return false
	}
	h.now = *h.pendingAlarm
	h.pendingAlarm = nil
	return true
}

func (h *mockHost) popReceived() *gpbft.MessageBuilder {
	if len(h.received) == 0 {
		return nil
	}
	mb := h.received[0]
	h.received = h.received[1:]
	return mb
}
