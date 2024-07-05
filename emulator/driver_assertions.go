package emulator

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-f3/gpbft"
)

// ValidTicket is a sentinel value to generate and set a valid gpbft.Ticket when
// delivering messages via Driver.RequireDeliverMessage.
var ValidTicket gpbft.Ticket = []byte("filled by driver when non-empty")

func (d *Driver) RequireDeliverMessage(message *gpbft.GMessage) {
	// A small hack to fill the ticket with the right value as whatever "right" is
	// dictated to be by signing. This keeps the signing swappable without ripple
	// effect across the driver.
	withTicket := bytes.Equal(message.Ticket, ValidTicket)
	instance := d.host.getInstance(message.Vote.Instance)
	d.require.NotNil(instance)
	mb := instance.NewMessageBuilder(message.Vote, message.Justification, withTicket)
	mb.SetNetworkName(d.host.NetworkName())
	mb.SetSigningMarshaler(d.host.adhocSigning)
	msg, err := mb.Build(context.Background(), d.host.adhocSigning, message.Sender)
	d.require.NoError(err)
	d.require.NotNil(msg)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireDeliverAlarm() {
	delivered, err := d.deliverAlarm()
	d.require.NoError(err)
	d.require.True(delivered)
}

func (d *Driver) RequireNoBroadcast() {
	d.require.Nil(d.host.popReceivedBroadcast())
}

func (d *Driver) RequireQuality() {
	msg := d.host.popReceivedBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.QUALITY_PHASE, msg.Vote.Step)
	d.require.Zero(msg.Vote.Round)
	d.require.Equal(instance.proposal, msg.Vote.Value)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Nil(msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequirePrepare(value gpbft.ECChain) {
	d.RequirePrepareAtRound(0, value, nil)
}

func (d *Driver) RequirePrepareAtRound(round uint64, value gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popReceivedBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.PREPARE_PHASE, msg.Vote.Step)
	d.require.Equal(round, msg.Vote.Round)
	d.require.Equal(value, msg.Vote.Value)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Equal(justification, msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireCommitForBottom(round uint64) {
	d.RequireCommit(round, gpbft.ECChain{}, nil)
}

func (d *Driver) RequireCommit(round uint64, vote gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popReceivedBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.COMMIT_PHASE, msg.Vote.Step)
	d.require.Equal(round, msg.Vote.Round)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(vote, msg.Vote.Value)
	d.require.Equal(justification, justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireConverge(round uint64, vote gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popReceivedBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.CONVERGE_PHASE, msg.Vote.Step)
	d.require.Equal(round, msg.Vote.Round)
	d.require.Equal(vote, msg.Vote.Value)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Equal(justification, msg.Justification)
	d.require.NotEmpty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireDecide(vote gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popReceivedBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.DECIDE_PHASE, msg.Vote.Step)
	d.require.Zero(msg.Vote.Round)
	d.require.Equal(vote, msg.Vote.Value)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Equal(justification, msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireDecision(instanceID uint64, expect gpbft.ECChain) {
	instance := d.host.getInstance(instanceID)
	d.require.NotNil(instance)
	decision := instance.GetDecision()
	d.require.NotNil(decision)
	d.require.Equal(expect, decision.Vote.Value)
}
