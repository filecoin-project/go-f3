package emulator

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

// Assertions is a utility wrapper around require.Assertions with the API
// tailored to asserting GPBFT interactions in order to reduce boilerplate code
// during testing.
type Assertions struct {
	*require.Assertions
}

// NewAssertions instantiates a new Assertions utility.
func NewAssertions(t *testing.T) *Assertions {
	return &Assertions{
		Assertions: require.New(t),
	}
}

// MessageReceived asserts that the given message is non-nil and err is nil. This
// function is typically used as wrapper to call Emulator.ReceiveBroadcast or
// Emulator.ReceiveMessage.
func (r *Assertions) MessageReceived(message *gpbft.GMessage, err error) *gpbft.GMessage {
	r.NoError(err)
	r.NotNil(message)
	return message
}

// AlarmReceived asserts that triggered is true with nil err, typically used as
// wrapper to call Emulator.ReceiveAlarm.
func (r *Assertions) AlarmReceived(triggered bool, err error) {
	r.NoError(err)
	r.True(triggered)
}

// Quality asserts that the given subject message belongs to the given instance
// and is a QUALITY step message voting for the instance's proposal.
func (r *Assertions) Quality(subject *gpbft.GMessage, instance *Instance) {
	r.Equal(gpbft.QUALITY_PHASE, subject.Vote.Step)
	r.Zero(subject.Vote.Round)
	r.Equal(instance.proposal, subject.Vote.Value)
	r.Equal(instance.id, subject.Vote.Instance)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Nil(subject.Justification)
	r.Empty(subject.Ticket)
}

// Prepare asserts that the given subject message belongs to the given instance
// and is a PREPARE step message voting for the instance's proposal.
func (r *Assertions) Prepare(subject *gpbft.GMessage, instance *Instance) {
	r.PrepareWithVoteAndJustification(subject, instance, 0, instance.GetProposal(), nil)
}

// PrepareWithVote asserts that the given subject message belongs to the given
// instance and is a PREPARE step message voting for the given value.
func (r *Assertions) PrepareWithVote(subject *gpbft.GMessage, instance *Instance, value gpbft.ECChain) {
	r.PrepareWithVoteAndJustification(subject, instance, 0, value, nil)
}

// PrepareWithVoteAndJustification asserts that the given subject message belongs
// to the given instance and is a PREPARE step message voting for the given value
// with the expected justification.
func (r *Assertions) PrepareWithVoteAndJustification(subject *gpbft.GMessage, instance *Instance, round uint64, value gpbft.ECChain, justification *gpbft.Justification) {
	r.Equal(gpbft.PREPARE_PHASE, subject.Vote.Step)
	r.Equal(round, subject.Vote.Round)
	r.Equal(value, subject.Vote.Value)
	r.Equal(instance.id, subject.Vote.Instance)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Equal(justification, subject.Justification)
	r.Empty(subject.Ticket)
}

// CommitWithJustification asserts that the given subject message belongs to the
// given instance, round and votes for the given vote with the expected
// justification.
func (r *Assertions) CommitWithJustification(subject *gpbft.GMessage, instance *Instance, round uint64, vote gpbft.ECChain, justification *gpbft.Justification) {
	r.Equal(gpbft.COMMIT_PHASE, subject.Vote.Step)
	r.Equal(round, subject.Vote.Round)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Equal(instance.id, subject.Vote.Instance)
	r.Equal(vote, subject.Vote.Value)
	r.Equal(justification, subject.Justification)
	r.Empty(subject.Ticket)
}

// CommitForBottom asserts that the given subject message belongs to the given
// instance, round and is a COMMIT for bottom.
func (r *Assertions) CommitForBottom(subject *gpbft.GMessage, instance *Instance, round uint64) {
	r.Equal(gpbft.COMMIT_PHASE, subject.Vote.Step)
	r.Equal(round, subject.Vote.Round)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Equal(instance.id, subject.Vote.Instance)
	r.True(subject.Vote.Value.IsZero())
	r.Nil(subject.Justification)
	r.Empty(subject.Ticket)
}

// Converge asserts that the given subject message belongs to the given instance,
// converges on the given vote and has the expected justification.
func (r *Assertions) Converge(subject *gpbft.GMessage, instance *Instance, round uint64, vote gpbft.ECChain, justification *gpbft.Justification) {
	r.Equal(gpbft.CONVERGE_PHASE, subject.Vote.Step)
	r.Equal(round, subject.Vote.Round)
	r.Equal(vote, subject.Vote.Value)
	r.Equal(instance.id, subject.Vote.Instance)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Equal(justification, subject.Justification)
	r.NotEmpty(subject.Ticket)
}

// Decide asserts that the given subject message belongs to the given instance,
// decides the given vote and has the expected justification.
func (r *Assertions) Decide(subject *gpbft.GMessage, instance *Instance, vote gpbft.ECChain, justification *gpbft.Justification) {
	r.Equal(gpbft.DECIDE_PHASE, subject.Vote.Step)
	r.Zero(subject.Vote.Round)
	r.Equal(vote, subject.Vote.Value)
	r.Equal(instance.id, subject.Vote.Instance)
	r.Equal(instance.supplementalData, subject.Vote.SupplementalData)
	r.Equal(justification, subject.Justification)
	r.Empty(subject.Ticket)
}

// Consensus asserts that the given instance has reached consensus for the expected chain.
func (r *Assertions) Consensus(instance *Instance, expect gpbft.ECChain) {
	decision := instance.GetDecision()
	r.NotNil(decision)
	r.Equal(expect, decision.Vote.Value)
}
