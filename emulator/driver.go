package emulator

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

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
	d.require.NoError(d.subject.StartInstance(id))
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

func (d *Driver) deliverMessage(msg *gpbft.GMessage) error {
	if validated, err := d.subject.ValidateMessage(msg); err != nil {
		return err
	} else {
		return d.subject.ReceiveMessage(validated)
	}
}
