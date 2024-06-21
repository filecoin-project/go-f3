package emulator

import (
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
)

// Emulator emulates the core GPBFT protocol for one honest participant, and
// allows frame-by-frame control over progress of a GPBFT instance.
//
// Progress may be made by calling Emulator.ReceiveMessage,
// Emulator.ReceiveBroadcast or Emulator.ReceiveAlarm.
type Emulator struct {
	subject *gpbft.Participant
	host    *host
}

// New instantiates a new Emulator with the given GPBFT options. See
// Emulator.Start.
func New(o ...gpbft.Option) (*Emulator, error) {
	h := newHost()
	participant, err := gpbft.NewParticipant(h, o...)
	if err != nil {
		return nil, err
	}
	return &Emulator{
		subject: participant,
		host:    h,
	}, nil
}

// Start starts emulation for the given instance and signals the start of
// instance to the emulated honest gpbft.Participant.
//
// See NewInstance.
func (e *Emulator) Start(instance *Instance) error {
	if err := e.host.setInstance(instance); err != nil {
		return err
	}
	return e.subject.StartInstance(instance.id)
}

// ReceiveAlarm triggers any pending alarms. Returns true if the alarm is
// triggered along with any errors that may occur as a result of triggering the
// alarm. Otherwise returns false with no error.
func (e *Emulator) ReceiveAlarm() (bool, error) {
	if e.host.maybeTriggerAlarm() {
		return true, e.subject.ReceiveAlarm()
	}
	return false, nil
}

// ReceiveMessage delivers a message to the emulated participant from the given
// sender. The sender must not be the ID of the honest participant as
// impersonation of messages to the participant is not supported.
//
// Returns the final built and delivered message along with any errors that may
// occur as a result of message delivery. If the building of the message fails
// nil message is returned with error.
func (e *Emulator) ReceiveMessage(from gpbft.ActorID, mb *gpbft.MessageBuilder) (*gpbft.GMessage, error) {
	if from == e.host.id {
		return nil, fmt.Errorf("subject impersonation is not supported")
	}
	mb.SetNetworkName(e.host.NetworkName())
	mb.SetSigningMarshaler(e.host.adhocSigning)
	msg, err := mb.Build(e.host.adhocSigning, from)
	if err != nil {
		return nil, err
	}
	return msg, e.doBroadcast(msg)
}

// ReceiveBroadcast delivers one pending broadcast, if any, requested by the
// emulated participant to itself. Returns the final built and delivered message
// along with any errors that may occur as part of message delivery. If there are
// no pending broadcasts returns nil as both the message and error.
func (e *Emulator) ReceiveBroadcast() (*gpbft.GMessage, error) {
	if len(e.host.pendingBroadcasts) != 0 {
		next := e.host.pendingBroadcasts[0]
		e.host.pendingBroadcasts = e.host.pendingBroadcasts[1:]
		return next, e.doBroadcast(next)
	}
	return nil, nil
}

func (e *Emulator) doBroadcast(msg *gpbft.GMessage) error {
	if validated, err := e.subject.ValidateMessage(msg); err != nil {
		return err
	} else {
		return e.subject.ReceiveMessage(validated)
	}
}
