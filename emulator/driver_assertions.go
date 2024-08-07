package emulator

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

func (d *Driver) RequireDeliverMessage(message *gpbft.GMessage) {
	msg := d.prepareMessage(message)
	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireErrOnDeliverMessage(message *gpbft.GMessage, err error, contains string) {
	msg := d.prepareMessage(message)
	gotErr := d.deliverMessage(msg)
	d.require.Error(gotErr)
	d.require.ErrorIs(gotErr, err)
	if contains != "" {
		d.require.ErrorContains(gotErr, contains)
	}
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
