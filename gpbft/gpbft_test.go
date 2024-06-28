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

	em := emulator.New(require)

	em.Start(instance)

	// Expect subject to emit QUALITY
	proposal := instance.GetProposal()
	// TODO: replace verifyBroadcast with VerifyQuality, VerifyPrepare etc that
	// have similar inspections to those removed from Assertions
	em.VerifyBroadcast(instance.NewQuality(proposal))

	// Deliver 1 QUALITY message, not enough to finish the phase.
	em.DeliverMessage(instance.MakeQuality(0, proposal))
	em.VerifyNoBroadcast()

	// Deliver another with same value, which ends the phase.
	em.DeliverMessage(instance.MakeQuality(1, proposal))
	// Expect PREPARE with the agreed value.
	em.VerifyBroadcast(instance.NewPrepare(proposal))

	// Deliver PREPAREs to the instance.
	em.DeliverMessage(instance.MakePrepare(0, 0, proposal))
	em.DeliverMessage(instance.MakePrepare(1, 0, proposal))

	// Optional: set mock expectation for signing aggregate by keys for participants 0, 1
	j1 := instance.MakeJustification(0, gpbft.PREPARE_PHASE, proposal, []uint64{0, 1})
	em.VerifyBroadcast(instance.NewCommit(proposal, j1))

	// Deliver COMMITs to the instance.
	em.DeliverMessage(instance.MakeCommit(0, 0, proposal, j1))
	em.DeliverMessage(instance.MakeCommit(1, 0, proposal, j1))

	// Expect broadcast of DECIDE.
	j2 := instance.MakeJustification(0, gpbft.COMMIT_PHASE, proposal, []uint64{0, 1})
	em.VerifyBroadcast(instance.NewDecide(proposal, j2))

	// Deliver DECIDEs to the instance.
	em.DeliverMessage(instance.MakeDecide(0, proposal, j2))
	em.DeliverMessage(instance.MakeDecide(1, proposal, j2))

	// Expect consensus is reached for instance proposal.
	require.Decided(instance, instance.GetProposal())
}
