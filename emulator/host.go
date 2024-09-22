package emulator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

const networkName = "emulator-net"

var _ gpbft.Host = (*driverHost)(nil)

type driverHost struct {
	Signing

	t                  *testing.T
	id                 gpbft.ActorID
	now                time.Time
	pendingAlarm       *time.Time
	receivedBroadcasts []*gpbft.GMessage
	chain              map[uint64]*Instance
}

func newHost(t *testing.T, signing Signing) *driverHost {
	return &driverHost{
		Signing: signing,
		t:       t,
		chain:   make(map[uint64]*Instance),
	}
}

func (h *driverHost) maybeReceiveAlarm() bool {
	if h.pendingAlarm == nil {
		return false
	}
	h.now = *h.pendingAlarm
	h.pendingAlarm = nil
	return true
}

func (h *driverHost) RequestBroadcast(mb *gpbft.MessageBuilder) error {
	msg, err := mb.Build(context.Background(), h, h.id)
	if err != nil {
		return err
	}
	h.receivedBroadcasts = append(h.receivedBroadcasts, msg)
	return nil
}

func (h *driverHost) ReceiveDecision(decision *gpbft.Justification) (time.Time, error) {
	require.NoError(h.t, h.maybeReceiveDecision(decision))
	return h.now, nil
}

func (h *driverHost) maybeReceiveDecision(decision *gpbft.Justification) error {
	switch instance := h.chain[decision.Vote.Instance]; {
	case instance == nil:
		return fmt.Errorf("cannot set decision for unknown instance ID: %d", decision.Vote.Instance)
	case instance.decision != nil:
		return fmt.Errorf("instance %d has already been decided", decision.Vote.Instance)
	default:
		instance.decision = decision
		return nil
	}
}

func (h *driverHost) GetProposal(id uint64) (*gpbft.SupplementalData, gpbft.ECChain, error) {
	instance := h.chain[id]
	if instance == nil {
		return nil, nil, fmt.Errorf("instance ID %d not found", id)
	}
	return &instance.supplementalData, instance.Proposal(), nil
}

func (h *driverHost) GetCommittee(id uint64) (*gpbft.Committee, error) {
	instance := h.chain[id]
	if instance == nil {
		return nil, fmt.Errorf("instance ID %d not found", id)
	}
	return &gpbft.Committee{
		PowerTable: instance.powerTable,
		Beacon:     instance.beacon,
	}, nil
}

func (h *driverHost) addInstance(instance *Instance) error {
	if existing := h.chain[instance.id]; existing != nil {
		return fmt.Errorf("instance ID %d is already set", instance.id)
	}
	h.chain[instance.id] = instance
	return nil
}

func (h *driverHost) getInstance(id uint64) *Instance {
	return h.chain[id]
}

func (h *driverHost) popNextBroadcast() *gpbft.GMessage {
	switch len(h.receivedBroadcasts) {
	case 0:
		return nil
	default:
		message := h.receivedBroadcasts[0]
		h.receivedBroadcasts = h.receivedBroadcasts[1:]
		return message
	}
}

func (h *driverHost) peekLastBroadcast() *gpbft.GMessage {
	switch l := len(h.receivedBroadcasts); l {
	case 0:
		return nil
	default:
		return h.receivedBroadcasts[l-1]
	}
}

func (h *driverHost) NetworkName() gpbft.NetworkName { return networkName }
func (h *driverHost) Time() time.Time                { return h.now }
func (h *driverHost) SetAlarm(at time.Time)          { h.pendingAlarm = &at }
