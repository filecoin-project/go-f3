package gpbft_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/gpbft/emulator"
)

var (
	tipset0 = gpbft.TipSet{Epoch: 0, Key: []byte("bigbang")}
	tipSet1 = gpbft.TipSet{Epoch: 1, Key: []byte("fish")}
	tipSet2 = gpbft.TipSet{Epoch: 2, Key: []byte("lobster")}
)

func TestGPBFT_DecidesProposalOnStrongEvidenceOfPrepare(t *testing.T) {
	require := emulator.NewAssertions(t)

	instance, err := emulator.NewInstance(
		0,
		gpbft.PowerEntries{
			gpbft.PowerEntry{
				ID:    0,
				Power: gpbft.NewStoragePower(1),
			},
			gpbft.PowerEntry{
				ID:    1,
				Power: gpbft.NewStoragePower(1),
			},
		},
		tipset0, tipSet1, tipSet2,
	)
	require.NoError(err)

	em, err := emulator.New()
	require.NoError(err)

	// Start the instance and trigger the first alarm because the GPBFT
	// implementation in this repo implicitly uses the alarm to actually start the
	// instance.
	require.NoError(em.Start(instance))
	require.AlarmReceived(em.ReceiveAlarm())

	// Expect QUALITY step from the emulator.
	require.Quality(require.MessageReceived(em.ReceiveBroadcast()), instance)

	// Send QUALITY to the emulator to satisfy strong quorum of senders.
	require.MessageReceived(
		em.ReceiveMessage(1, instance.NewQualityForProposal()),
	)

	// Expect PREPARE step with the original instance proposal, since strong quorum
	// of senders is met during QUALITY phase.
	receivedPrepare := require.MessageReceived(em.ReceiveBroadcast())
	require.Prepare(receivedPrepare, instance)

	// Send PREPARE message to the emulator for the instance proposal which should
	// facilitate evidence of strong quorum of PREPARE.
	sentPrepare := require.MessageReceived(
		em.ReceiveMessage(1, instance.NewPrepareForProposal()),
	)

	// Construct the expected evidence of strong quorum of PREPARE for instance proposal.
	wantPrepareEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.PREPARE_PHASE, instance.GetProposal()),
		[]int{0, 1}, receivedPrepare.Signature, sentPrepare.Signature)
	require.NoError(err)

	// Expect COMMIT step with the evidence of strong quorum of PREPARE for instance
	// proposal.
	receivedCommit := require.MessageReceived(em.ReceiveBroadcast())
	require.CommitWithJustification(
		receivedCommit,
		instance,
		0,
		instance.GetProposal(),
		wantPrepareEvidence,
	)

	// Send COMMIT message with strong quorum of PREPARE to satisfy strong quorum of
	// senders.
	sentCommit := require.MessageReceived(
		em.ReceiveMessage(1,
			instance.NewMessageBuilder(gpbft.Payload{
				Step:  gpbft.COMMIT_PHASE,
				Value: instance.GetProposal(),
			}, wantPrepareEvidence, false)),
	)
	require.NoError(err)

	// Construct the expected evidence of strong quorum of COMMIT for instance proposal.
	wantDecideEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.COMMIT_PHASE, instance.GetProposal()),
		[]int{0, 1}, receivedCommit.Signature, sentCommit.Signature)
	require.NoError(err)

	// Expect DECIDE step with the evidence of strong quorum of COMMIT for instance
	// proposal.
	require.Decide(
		require.MessageReceived(em.ReceiveBroadcast()),
		instance,
		instance.GetProposal(),
		wantDecideEvidence,
	)

	// Send DECIDE message with strong quorum of COMMIT to satisfy strong quorum of
	// senders.
	require.MessageReceived(em.ReceiveMessage(1,
		instance.NewMessageBuilder(gpbft.Payload{
			Step:  gpbft.DECIDE_PHASE,
			Value: instance.GetProposal(),
		}, wantDecideEvidence, false)),
	)

	// Expect consensus is reached for instance proposal.
	require.Consensus(instance, instance.GetProposal())
}

func TestGPBFT_DecidesBaseOnNoStrongQuorum(t *testing.T) {
	require := emulator.NewAssertions(t)

	instance, err := emulator.NewInstance(
		0,
		gpbft.PowerEntries{
			gpbft.PowerEntry{
				ID:    0,
				Power: gpbft.NewStoragePower(1),
			},
			gpbft.PowerEntry{
				ID:    1,
				Power: gpbft.NewStoragePower(1),
			},
		},
		tipset0, tipSet1, tipSet2,
	)
	require.NoError(err)

	em, err := emulator.New()
	require.NoError(err)

	baseChain := instance.GetProposal().BaseChain()
	alternativeProposal := baseChain.Extend([]byte("barreleye"))

	// Start the instance and trigger the first alarm because the GPBFT
	// implementation in this repo implicitly uses the alarm to actually start the
	// instance.
	require.NoError(em.Start(instance))
	require.AlarmReceived(em.ReceiveAlarm())

	// Expect QUALITY step from the emulator.
	require.Quality(require.MessageReceived(em.ReceiveBroadcast()), instance)

	// Send QUALITY message for alternative proposal
	require.MessageReceived(
		em.ReceiveMessage(1, instance.NewQuality(alternativeProposal)),
	)

	// Trigger Alarm to force begin PREPARE.
	require.AlarmReceived(em.ReceiveAlarm())

	// Expect PREPARE for the proposal base chain.
	receivedPrepare := require.MessageReceived(em.ReceiveBroadcast())
	require.PrepareWithVote(receivedPrepare, instance, baseChain)

	// Send PREPARE for proposal base chain.
	sentPrepare := require.MessageReceived(
		em.ReceiveMessage(1, instance.NewPrepare(baseChain)),
	)

	// Construct the expected evidence of strong quorum of PREPARE for base chain.
	wantPrepareEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.PREPARE_PHASE, baseChain),
		[]int{0, 1}, receivedPrepare.Signature, sentPrepare.Signature)
	require.NoError(err)

	// Expect COMMIT step with the evidence of strong quorum of PREPARE for base
	// chain.
	receivedCommit := require.MessageReceived(em.ReceiveBroadcast())
	require.CommitWithJustification(
		receivedCommit,
		instance,
		0,
		baseChain,
		wantPrepareEvidence,
	)
	// Send COMMIT message with strong quorum of PREPARE to satisfy strong quorum of
	// senders.
	sentCommit := require.MessageReceived(
		em.ReceiveMessage(1,
			instance.NewMessageBuilder(gpbft.Payload{
				Step:  gpbft.COMMIT_PHASE,
				Value: baseChain,
			}, wantPrepareEvidence, false)),
	)
	require.NoError(err)

	// Construct the expected evidence of strong quorum of COMMIT for base chain.
	wantDecideEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.COMMIT_PHASE, baseChain),
		[]int{0, 1}, receivedCommit.Signature, sentCommit.Signature)
	require.NoError(err)

	// Expect DECIDE step with the evidence of strong quorum of COMMIT for base
	// chain.
	require.Decide(
		require.MessageReceived(em.ReceiveBroadcast()),
		instance,
		baseChain,
		wantDecideEvidence,
	)

	// Send DECIDE message with strong quorum of COMMIT to satisfy strong quorum of
	// senders.
	require.MessageReceived(em.ReceiveMessage(1,
		instance.NewMessageBuilder(gpbft.Payload{
			Step:  gpbft.DECIDE_PHASE,
			Value: baseChain,
		}, wantDecideEvidence, false)),
	)

	// Expect consensus is reached for instance proposal.
	require.Consensus(instance, baseChain)
}

func TestGPBFT_DecidesBaseOnConvergeWhenQuorumNotPossible(t *testing.T) {
	require := emulator.NewAssertions(t)

	instance, err := emulator.NewInstance(
		0,
		gpbft.PowerEntries{
			gpbft.PowerEntry{
				ID:    0,
				Power: gpbft.NewStoragePower(1),
			},
			gpbft.PowerEntry{
				ID:    1,
				Power: gpbft.NewStoragePower(1),
			},
		},
		tipset0, tipSet1, tipSet2,
	)
	require.NoError(err)

	em, err := emulator.New()
	require.NoError(err)

	baseChain := instance.GetProposal().BaseChain()
	alternativeProposal := baseChain.Extend([]byte("barreleye"))

	// Start the instance and trigger the first alarm because the GPBFT
	// implementation in this repo implicitly uses the alarm to actually start the
	// instance.
	require.NoError(em.Start(instance))
	require.AlarmReceived(em.ReceiveAlarm())

	// Expect QUALITY step from the emulator.
	require.Quality(require.MessageReceived(em.ReceiveBroadcast()), instance)

	// Send QUALITY message for alternative proposal
	require.MessageReceived(
		em.ReceiveMessage(1, instance.NewQuality(alternativeProposal)),
	)

	// Trigger Alarm to force begin PREPARE.
	require.AlarmReceived(em.ReceiveAlarm())

	// Expect PREPARE for the proposal base chain.
	receivedPrepare := require.MessageReceived(em.ReceiveBroadcast())
	require.PrepareWithVote(receivedPrepare, instance, baseChain)

	// Send PREPARE for alternative proposal, which should make quorum impossible.
	require.MessageReceived(
		em.ReceiveMessage(1, instance.NewPrepare(alternativeProposal)),
	)

	// Expect COMMIT for bottom.
	receivedCommit := require.MessageReceived(em.ReceiveBroadcast())
	require.CommitForBottom(
		receivedCommit,
		instance,
		0,
	)
	// Send COMMIT message with strong quorum of PREPARE to satisfy strong quorum of
	// senders.
	sentCommit := require.MessageReceived(
		em.ReceiveMessage(1,
			instance.NewMessageBuilder(gpbft.Payload{
				Step: gpbft.COMMIT_PHASE,
			}, nil, false)),
	)
	require.NoError(err)

	// Construct the expected evidence of strong quorum of COMMIT for bottom.
	wantConvergeEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.COMMIT_PHASE, gpbft.ECChain{}),
		[]int{0, 1}, receivedCommit.Signature, sentCommit.Signature)
	require.NoError(err)

	// Expect CONVERGE step at next round with the evidence of strong quorum of
	// COMMIT for bottom.
	require.Converge(
		require.MessageReceived(em.ReceiveBroadcast()),
		instance,
		1,
		baseChain,
		wantConvergeEvidence,
	)

	require.MessageReceived(
		em.ReceiveMessage(1, instance.NewMessageBuilder(
			instance.NewPayload(1, gpbft.CONVERGE_PHASE, baseChain), wantConvergeEvidence, true)),
	)

	// Trigger timeout to facilitate progress of CONVERGE phase.
	require.AlarmReceived(em.ReceiveAlarm())

	receivedPrepare = require.MessageReceived(em.ReceiveBroadcast())
	require.PrepareWithVoteAndJustification(
		receivedPrepare,
		instance,
		1,
		baseChain,
		wantConvergeEvidence,
	)
	sentPrepare := require.MessageReceived(
		em.ReceiveMessage(1, instance.NewMessageBuilder(instance.NewPayload(1, gpbft.PREPARE_PHASE, baseChain), wantConvergeEvidence, false)),
	)

	wantCommitEvidence, err := instance.NewJustification(
		instance.NewPayload(1, gpbft.PREPARE_PHASE, baseChain), []int{0, 1}, receivedPrepare.Signature, sentPrepare.Signature,
	)
	require.NoError(err)

	receivedCommit = require.MessageReceived(em.ReceiveBroadcast())
	require.CommitWithJustification(
		receivedCommit,
		instance,
		1,
		baseChain,
		wantCommitEvidence,
	)

	sentCommit = require.MessageReceived(
		em.ReceiveMessage(1,
			instance.NewMessageBuilder(instance.NewPayload(1, gpbft.COMMIT_PHASE, baseChain),
				wantCommitEvidence, false,
			)),
	)

	wantDecideEvidence, err := instance.NewJustification(
		instance.NewPayload(1, gpbft.COMMIT_PHASE, baseChain),
		[]int{0, 1},
		receivedCommit.Signature, sentCommit.Signature)
	require.NoError(err)
	// Expect DECIDE step with the evidence of strong quorum of COMMIT for base
	// chain.
	require.Decide(
		require.MessageReceived(em.ReceiveBroadcast()),
		instance,
		baseChain,
		wantDecideEvidence,
	)

	// Send DECIDE message with strong quorum of COMMIT to satisfy strong quorum of
	// senders.
	require.MessageReceived(em.ReceiveMessage(1,
		instance.NewMessageBuilder(gpbft.Payload{
			Step:  gpbft.DECIDE_PHASE,
			Value: baseChain,
		}, wantDecideEvidence, false)),
	)

	// Expect consensus is reached for instance proposal.
	require.Consensus(instance, baseChain)
}

func TestGPBFT_SoleParticipantWithNoTimeoutDecidesOnProposal(t *testing.T) {
	require := emulator.NewAssertions(t)

	instance, err := emulator.NewInstance(
		0,
		gpbft.PowerEntries{
			gpbft.PowerEntry{
				ID:    0,
				Power: gpbft.NewStoragePower(1),
			},
		},
		tipset0, tipSet1, tipSet2,
	)
	require.NoError(err)

	em, err := emulator.New()
	require.NoError(err)

	require.NoError(em.Start(instance))
	require.AlarmReceived(em.ReceiveAlarm())

	require.Quality(require.MessageReceived(em.ReceiveBroadcast()), instance)
	receivedPrepare := require.MessageReceived(em.ReceiveBroadcast())
	require.Prepare(receivedPrepare, instance)

	wantPrepareEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.PREPARE_PHASE, instance.GetProposal()), []int{0}, receivedPrepare.Signature)
	require.NoError(err)
	receivedCommit := require.MessageReceived(em.ReceiveBroadcast())
	require.CommitWithJustification(receivedCommit, instance, 0, instance.GetProposal(), wantPrepareEvidence)

	wantCommitEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.COMMIT_PHASE, instance.GetProposal()), []int{0}, receivedCommit.Signature,
	)
	require.NoError(err)
	require.Decide(require.MessageReceived(em.ReceiveBroadcast()), instance, instance.GetProposal(), wantCommitEvidence)
	require.Consensus(instance, instance.GetProposal())
}

func TestGPBFT_SoleParticipantWithTimeoutQualityStepDecidesOnBase(t *testing.T) {
	require := emulator.NewAssertions(t)

	instance, err := emulator.NewInstance(
		0,
		gpbft.PowerEntries{
			gpbft.PowerEntry{
				ID:    0,
				Power: gpbft.NewStoragePower(1),
			},
		},
		tipset0, tipSet1, tipSet2,
	)
	require.NoError(err)
	baseChain := instance.GetProposal().BaseChain()

	em, err := emulator.New()
	require.NoError(err)

	require.NoError(em.Start(instance))
	require.AlarmReceived(em.ReceiveAlarm())

	// Trigger timeout at QUALITY phase
	require.AlarmReceived(em.ReceiveAlarm())
	require.Quality(require.MessageReceived(em.ReceiveBroadcast()), instance)

	receivedPrepare := require.MessageReceived(em.ReceiveBroadcast())
	require.PrepareWithVote(receivedPrepare, instance, baseChain)

	wantPrepareEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.PREPARE_PHASE, baseChain), []int{0}, receivedPrepare.Signature)
	require.NoError(err)
	receivedCommit := require.MessageReceived(em.ReceiveBroadcast())
	require.CommitWithJustification(receivedCommit, instance, 0, baseChain, wantPrepareEvidence)

	wantCommitEvidence, err := instance.NewJustification(
		instance.NewPayload(0, gpbft.COMMIT_PHASE, baseChain), []int{0}, receivedCommit.Signature,
	)
	require.NoError(err)
	require.Decide(require.MessageReceived(em.ReceiveBroadcast()), instance, baseChain, wantCommitEvidence)
	require.Consensus(instance, baseChain)
}
