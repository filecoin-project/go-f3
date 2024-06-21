package emulator

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ gpbft.Host = (*host)(nil)

type host struct {
	chain
	adhocSigning
	id                gpbft.ActorID
	now               time.Time
	pendingAlarm      *time.Time
	pendingBroadcasts []*gpbft.GMessage
}

func newHost() *host {
	return &host{
		chain: newChain(),
	}
}

func (h *host) maybeTriggerAlarm() bool {
	if h.pendingAlarm == nil {
		return false
	}
	h.now = *h.pendingAlarm
	h.pendingAlarm = nil
	return true
}

func (h *host) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	mb.SetNetworkName(h.NetworkName())
	mb.SetSigningMarshaler(h)
	msg, err := mb.Build(h, h.id)
	if err != nil {
		return err
	}
	h.pendingBroadcasts = append(h.pendingBroadcasts, msg)
	return nil
}

func (h *host) ReceiveDecision(decision *gpbft.Justification) time.Time {
	if err := h.setDecision(decision); err != nil {
		// TODO: add the ability to set callback and make emulated behavior here configurable.
		panic(err)
	}
	return h.now
}

func (h *host) NetworkName() gpbft.NetworkName { return "emulator-net" }
func (h *host) Time() time.Time                { return h.now }
func (h *host) SetAlarm(at time.Time)          { h.pendingAlarm = &at }
