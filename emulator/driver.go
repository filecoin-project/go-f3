package emulator

import (
	"bytes"
	"context"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

// ValidTicket is a sentinel value to generate and set a valid gpbft.Ticket when
// delivering messages via Driver.deliverMessage.
var ValidTicket gpbft.Ticket = []byte("filled in by driver")

// Driver drives the emulation of a GPBFT instance for one honest participant,
// and allows frame-by-frame control over progress of a GPBFT instance.
type Driver struct {
	require *require.Assertions
	subject *gpbft.Participant
	host    *driverHost
}

// NewDriver instantiates a new Driver with the given GPBFT options. See
// Driver.StartInstance.
func NewDriver(t *testing.T, o ...gpbft.Option) *Driver {
	h := newHost(t)
	participant, err := gpbft.NewParticipant(h, o...)
	require.NoError(t, err)
	return &Driver{
		require: require.New(t),
		subject: participant,
		host:    h,
	}
}

// StartInstance sets the current instances and starts emulation for it by signalling the
// start of instance to the emulated honest gpbft.Participant.
//
// See NewInstance.
func (d *Driver) StartInstance(id uint64) {
	d.require.NoError(d.subject.StartInstanceAt(id, d.host.Time()))
	// Trigger alarm once based on the implicit assumption that go-f3 uses alarm to
	// kickstart an instance internally.
	d.RequireDeliverAlarm()
}

// AddInstance adds an instance to the list of instances known by the driver.
func (d *Driver) AddInstance(instance *Instance) {
	d.require.NoError(d.host.addInstance(instance))
}

func (d *Driver) deliverAlarm() (bool, error) {
	if d.host.maybeReceiveAlarm() {
		return true, d.subject.ReceiveAlarm()
	}
	return false, nil
}

// prepareMessage prepares a partial message for delivery to the subject
// participant. If the given message has the sentinel ValidTicket set as its
// ticket then the Driver will prepare the final message with a valid ticket.
// Otherwise, ticket from partial messages is set as is on the final prepared
// message.
func (d *Driver) prepareMessage(partialMessage *gpbft.GMessage) *gpbft.GMessage {
	// A small hack to fill the ticket with the right value as whatever "right" is
	// dictated to be by signing. This keeps the signing swappable without ripple
	// effect across the driver.
	withValidTicket := bytes.Equal(partialMessage.Ticket, ValidTicket)
	instance := d.host.getInstance(partialMessage.Vote.Instance)
	d.require.NotNil(instance)

	mb := instance.NewMessageBuilder(partialMessage.Vote, partialMessage.Justification, withValidTicket)
	mb.NetworkName = d.host.NetworkName()
	mb.SigningMarshaler = d.host.adhocSigning
	msg, err := mb.Build(context.Background(), d.host.adhocSigning, partialMessage.Sender)
	d.require.NoError(err)
	d.require.NotNil(msg)
	if !withValidTicket {
		msg.Ticket = partialMessage.Ticket
	}
	return msg
}

func (d *Driver) deliverMessage(msg *gpbft.GMessage) error {
	if validated, err := d.subject.ValidateMessage(msg); err != nil {
		return err
	} else {
		return d.subject.ReceiveMessage(validated)
	}
}
