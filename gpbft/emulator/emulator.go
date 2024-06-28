package emulator

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/gpbft"
	"slices"
)

// Emulator emulates the core GPBFT protocol for one honest participant, and
// allows frame-by-frame control over progress of a GPBFT instance.
//
// Progress may be made by calling Emulator.DeliverMessage,
// Emulator.ReceiveBroadcast or Emulator.DeliverAlarm.
type Driver struct {
	subject *gpbft.Participant
	host    *mockHost
	require *Assertions
}

// New instantiates a new Emulator with the given GPBFT options. See
// Emulator.Start.
func New(assertions *Assertions, o ...gpbft.Option) *Driver {
	h := newHost()
	participant, err := gpbft.NewParticipant(h, o...)
	assertions.NoError(err)
	return &Driver{
		subject: participant,
		host:    h,
		require: assertions,
	}
}

// Start starts emulation for the given instance.
// It signals the start of instance to the subject and delivers the initial alarm.
func (e *Driver) Start(instance *Instance) {
	e.require.NoError(e.host.setInstance(instance))
	e.require.NoError(e.subject.StartInstance(instance.id))
	// The participant always sets an alarm during which it will call back for the chain etc.
	e.DeliverAlarm()
}

// DeliverAlarm triggers a pending alarm.
// Fails if there is is none Returns true if the alarm is
// triggered along with any errors that may occur as a result of triggering the
// alarm. Otherwise returns false with no error.
func (e *Driver) DeliverAlarm() {
	if e.host.maybeConsumeAlarm() {
		e.require.NoError(e.subject.ReceiveAlarm())
	} else {
		e.require.Fail("no alarm pending")
	}
}

// DeliverMessage delivers a message to the participant.
//
// Returns the final built and delivered message along with any errors that may
// occur as a result of message delivery. If the building of the message fails
// nil message is returned with error.
func (e *Driver) DeliverMessage(msg *gpbft.GMessage) {
	e.require.NoError(e.deliverMessage(msg))
}

func (e *Driver) deliverMessage(msg *gpbft.GMessage) error {
	if validated, err := e.subject.ValidateMessage(msg); err != nil {
		return err
	} else {
		return e.subject.ReceiveMessage(validated)
	}
}

func (e *Driver) VerifyBroadcast(mb *gpbft.MessageBuilder) {
	r := e.host.popReceived()
	e.require.NotNil(r, "no broadcast received")
	e.require.Equal(mb.Payload(), r.Payload())
	if mb.Justification() != nil {
		e.require.Equal(mb.Justification().Vote, r.Justification().Vote)
		e.require.True(bitfieldsEqual(mb.Justification().Signers, r.Justification().Signers))
		// XXX is signature even worth checking here?
		// Instead if wanted the mock host can verify the inputs to Sign/Aggregate.
		e.require.Equal(mb.Justification().Signature, r.Justification().Signature)
	} else {
		e.require.Nil(r.Justification())
	}
}

func (e *Driver) VerifyNoBroadcast() {
	e.require.Nil(e.host.popReceived(), "unexpected broadcast received")
}

func bitfieldsEqual(a, b bitfield.BitField) bool {
	ab, err := a.All(1000)
	if err != nil {
		return false
	}
	bb, err := b.All(1000)
	if err != nil {
		return false
	}
	return slices.Equal(ab, bb)
}
