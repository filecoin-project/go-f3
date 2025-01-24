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

// RequireStartInstance asserts that instance with the given ID is started. See
// StartInstance.
func (d *Driver) RequireStartInstance(id uint64) {
	d.require.NoError(d.StartInstance(id))
}

func (d *Driver) RequireDeliverAlarm() {
	delivered, err := d.DeliverAlarm()
	d.require.NoError(err)
	d.require.True(delivered)
}

func (d *Driver) RequireNoBroadcast() {
	d.require.Nil(d.host.popNextBroadcast())
}

func (d *Driver) RequireQuality() {
	msg := d.host.popNextBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.QUALITY_PHASE, msg.Vote.Phase)
	d.require.Zero(msg.Vote.Round)
	d.require.True(instance.proposal.Eq(msg.Vote.Value))
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Nil(msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequirePrepare(value *gpbft.ECChain) {
	d.RequirePrepareAtRound(0, value, nil)
}

func (d *Driver) RequirePrepareAtRound(round uint64, value *gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popNextBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.PREPARE_PHASE, msg.Vote.Phase)
	d.require.Equal(round, msg.Vote.Round)
	d.require.True(value.Eq(msg.Vote.Value))
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.requireEqualJustification(justification, msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireCommitForBottom(round uint64) {
	d.RequireCommit(round, &gpbft.ECChain{}, nil)
}

func (d *Driver) RequireCommit(round uint64, vote *gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popNextBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.COMMIT_PHASE, msg.Vote.Phase)
	d.require.Equal(round, msg.Vote.Round)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.True(vote.Eq(msg.Vote.Value))
	d.requireEqualJustification(justification, justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireConverge(round uint64, vote *gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popNextBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.CONVERGE_PHASE, msg.Vote.Phase)
	d.require.Equal(round, msg.Vote.Round)
	d.require.True(vote.Eq(msg.Vote.Value))
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.requireEqualJustification(justification, msg.Justification)
	d.require.NotEmpty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) requireEqualJustification(one *gpbft.Justification, other *gpbft.Justification) {
	if one == nil || other == nil {
		d.require.Equal(one, other)
	} else {
		d.require.Equal(one.Signature, other.Signature)
		d.require.Equal(one.Signers, other.Signers)
		d.require.True(one.Vote.Eq(&other.Vote))
	}
}

func (d *Driver) RequireDecide(vote *gpbft.ECChain, justification *gpbft.Justification) {
	msg := d.host.popNextBroadcast()
	d.require.NotNil(msg)
	instance := d.host.getInstance(msg.Vote.Instance)
	d.require.NotNil(instance)
	d.require.Equal(gpbft.DECIDE_PHASE, msg.Vote.Phase)
	d.require.Zero(msg.Vote.Round)
	d.require.True(vote.Eq(msg.Vote.Value))
	d.require.Equal(instance.id, msg.Vote.Instance)
	d.require.Equal(instance.supplementalData, msg.Vote.SupplementalData)
	d.requireEqualJustification(justification, msg.Justification)
	d.require.Empty(msg.Ticket)

	d.require.NoError(d.deliverMessage(msg))
}

func (d *Driver) RequireDecision(instanceID uint64, expect *gpbft.ECChain) {
	instance := d.host.getInstance(instanceID)
	d.require.NotNil(instance)
	decision := instance.GetDecision()
	d.require.NotNil(decision)
	d.require.True(expect.Eq(decision.Vote.Value))
}

// RequirePeekAtLastVote asserts that the last message broadcasted by the subject
// participant was for the given phase, round and vote.
func (d *Driver) RequirePeekAtLastVote(phase gpbft.Phase, round uint64, vote *gpbft.ECChain) {
	last := d.host.peekLastBroadcast()
	d.require.Equal(phase, last.Vote.Phase, "Expected last vote phase %s, but got %s", phase, last.Vote.Phase)
	d.require.Equal(round, last.Vote.Round, "Expected last vote round %d, but got %d", round, last.Vote.Round)
	d.require.True(vote.Eq(last.Vote.Value), "Expected last vote value %s, but got %s", vote, last.Vote.Value)
}
