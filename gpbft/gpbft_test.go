package gpbft_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/emulator"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

var (
	tipset0 = &gpbft.TipSet{Epoch: 0, Key: []byte("bigbang"), PowerTable: ptCid}
	tipSet1 = &gpbft.TipSet{Epoch: 1, Key: []byte("fish"), PowerTable: ptCid}
	tipSet2 = &gpbft.TipSet{Epoch: 2, Key: []byte("lobster"), PowerTable: ptCid}
	tipSet3 = &gpbft.TipSet{Epoch: 3, Key: []byte("fisherman"), PowerTable: ptCid}
	tipSet4 = &gpbft.TipSet{Epoch: 4, Key: []byte("lobstermucher"), PowerTable: ptCid}
)

func TestGPBFT_UnevenPowerDistribution(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
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
				gpbft.PowerEntry{
					ID:    2,
					Power: gpbft.NewStoragePower(2),
				},
				gpbft.PowerEntry{
					ID:    3,
					Power: gpbft.NewStoragePower(3),
				},
				gpbft.PowerEntry{
					ID:    4,
					Power: gpbft.NewStoragePower(5),
				},
			},
			tipset0, tipSet1, tipSet2, tipSet3,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Late arriving COMMIT produces decision at future round", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)

		baseChain := instance.Proposal().BaseChain()
		proposal1 := baseChain.Extend(tipSet1.Key)
		proposal2 := proposal1.Extend(tipSet2.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(proposal1),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 2,
			Vote:   instance.NewQuality(proposal2),
		})
		driver.RequireDeliverAlarm()

		driver.RequirePrepare(baseChain)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, proposal1),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 4,
			Vote:   instance.NewPrepare(0, proposal2),
		})

		driver.RequireCommitForBottom(0)
		evidenceOfPrepareForProposal2AtRound10 := instance.NewJustification(10, gpbft.PREPARE_PHASE, proposal2, 4, 3)
		commitToProposal2AtRound10 := instance.NewCommit(10, proposal2)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        4,
			Vote:          commitToProposal2AtRound10,
			Justification: evidenceOfPrepareForProposal2AtRound10,
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        3,
			Vote:          commitToProposal2AtRound10,
			Justification: evidenceOfPrepareForProposal2AtRound10,
		})
		driver.RequireDecide(proposal2, instance.NewJustification(10, gpbft.COMMIT_PHASE, proposal2, 4, 3))
	})
}

func TestGPBFT_WithEvenPowerDistribution(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
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
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Decides proposal on strong quorum", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(instance.Proposal()),
		})
		driver.RequirePrepare(instance.Proposal())
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})

		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, instance.Proposal()),
			Justification: evidenceOfPrepare,
		})

		evidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDecide(instance.Proposal(), evidenceOfCommit)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, instance.Proposal()),
			Justification: evidenceOfCommit,
		})

		driver.RequireDecision(instance.ID(), instance.Proposal())
	})

	t.Run("Decides base on lack of quorum", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		baseChain := instance.Proposal().BaseChain()
		alternativeProposal := baseChain.Extend([]byte("barreleye"))

		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(alternativeProposal),
		})
		driver.RequireDeliverAlarm()
		driver.RequirePrepare(baseChain)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, baseChain),
		})
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 0, 1)

		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, baseChain),
			Justification: evidenceOfPrepare,
		})

		evidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, baseChain, 0, 1)
		driver.RequireDecide(baseChain, evidenceOfCommit)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, baseChain),
			Justification: evidenceOfCommit,
		})
		driver.RequireDecision(instance.ID(), baseChain)
	})

	t.Run("Converges on base when quorum not possible", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		baseChain := instance.Proposal().BaseChain()
		alternativeProposal := baseChain.Extend([]byte("barreleye"))

		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(alternativeProposal),
		})
		driver.RequireDeliverAlarm()
		driver.RequirePrepare(baseChain)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, alternativeProposal),
		})

		driver.RequireCommitForBottom(0)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewCommit(0, &gpbft.ECChain{}),
		})

		evidenceOfCommitForBottom := instance.NewJustification(0, gpbft.COMMIT_PHASE, nil, 0, 1)

		driver.RequireConverge(1, baseChain, evidenceOfCommitForBottom)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(1, baseChain),
			Justification: evidenceOfCommitForBottom,
			Ticket:        emulator.ValidTicket,
		})
		driver.RequireDeliverAlarm()

		driver.RequirePrepareAtRound(1, baseChain, evidenceOfCommitForBottom)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(1, baseChain),
			Justification: evidenceOfCommitForBottom,
		})

		evidenceOfPrepare := instance.NewJustification(1, gpbft.PREPARE_PHASE, baseChain, 0, 1)

		driver.RequireCommit(1, baseChain, evidenceOfPrepare)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(1, baseChain),
			Justification: evidenceOfPrepare,
		})

		evidenceOfCommit := instance.NewJustification(1, gpbft.COMMIT_PHASE, baseChain, 0, 1)
		driver.RequireDecide(baseChain, evidenceOfCommit)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, baseChain),
			Justification: evidenceOfCommit,
		})
		driver.RequireDecision(instance.ID(), baseChain)
	})

	t.Run("Dequeues received messages after start", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(instance.Proposal()),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(instance.Proposal())
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
	})

	t.Run("Queues future instance messages during current instance", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureInstance := emulator.NewInstance(t,
			8,
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
			tipSet3, tipSet4,
		)
		driver.AddInstance(futureInstance)
		driver.RequireStartInstance(instance.ID())
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(instance.Proposal()),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})

		// Send messages from future instance which should be enough to progress the
		// future instance to COMMIT with strong evidence of prepare without any further
		// messages or timeout.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   futureInstance.NewQuality(futureInstance.Proposal()),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   futureInstance.NewPrepare(0, futureInstance.Proposal()),
		})

		driver.RequireQuality()
		driver.RequirePrepare(instance.Proposal())
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, instance.Proposal()),
			Justification: evidenceOfPrepare,
		})
		evidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDecide(instance.Proposal(), evidenceOfCommit)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, instance.Proposal()),
			Justification: evidenceOfCommit,
		})
		driver.RequireDecision(instance.ID(), instance.Proposal())

		// Start the future instance and expect progress to commit without any timeouts
		// based on previously queued messages.
		driver.RequireStartInstance(futureInstance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(futureInstance.Proposal())
		driver.RequireCommit(0, futureInstance.Proposal(), futureInstance.NewJustification(0, gpbft.PREPARE_PHASE, futureInstance.Proposal(), 0, 1))
	})

	t.Run("Rebroadcasts selected messages on timeout", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()
		driver.RequireDeliverAlarm()

		baseChain := instance.Proposal().BaseChain()
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()
		// Trigger alarm to necessitate the need for scheduling rebroadcast.
		driver.RequireDeliverAlarm()
		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger rebroadcast alarm.
		driver.RequireDeliverAlarm()
		// Expect rebroadcast of QUALITY and PREPARE messages.
		driver.RequireQuality()
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()
		// Trigger alarm and expect immediate rebroadcast of PREMARE.
		driver.RequireDeliverAlarm()
		// Expect rebroadcast of QUALITY and PREPARE messages.
		driver.RequireQuality()
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()

		// Deliver PREPARE for bottom to facilitate progress to COMMIT.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, baseChain),
		})

		// Expect progress to COMMIT with strong evidence of PREPARE.
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 0, 1)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)

		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger alarm to necessitate the need for scheduling rebroadcast, since
		// rebroadcast timeout must have been reset Due to progress to COMMIT.
		driver.RequireDeliverAlarm()
		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger rebroadcast alarm.
		driver.RequireDeliverAlarm()

		// Expect rebroadcast of QUALITY, PREPARE and COMMIT.
		driver.RequireQuality()
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()

		// Deliver COMMIT for bottom to facilitate progress to CONVERGE.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewCommit(0, &gpbft.ECChain{}),
		})

		// Expect Converge with evidence of COMMIT for bottom.
		evidenceOfPrepareForBase := instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 0, 1)
		driver.RequireConverge(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireNoBroadcast()

		// Trigger alarm to facilitate PREPARE at round 1.
		driver.RequireDeliverAlarm()
		driver.RequirePrepareAtRound(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireNoBroadcast()

		// Trigger alarm to necessitate the need for scheduling rebroadcast, since
		// rebroadcast timeout must have been reset now that progress is made.
		driver.RequireDeliverAlarm()
		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger rebroadcast alarm.
		driver.RequireDeliverAlarm()

		// Expect rebroadcast of all messages from previous and current round, including QUALITY.
		driver.RequireQuality()
		driver.RequirePrepareAtRound(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireConverge(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequirePrepare(baseChain)

		// Deliver PREPARE for base to facilitate progress to COMMIT.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(1, baseChain),
			Justification: evidenceOfPrepareForBase,
		})

		// Expect Progress to COMMIT at round 1.
		evidenceOfPrepareAtRound1 := instance.NewJustification(1, gpbft.PREPARE_PHASE, baseChain, 0, 1)
		driver.RequireCommit(1, baseChain, evidenceOfPrepareAtRound1)
		driver.RequireNoBroadcast()

		// Trigger alarm to necessitate the need for scheduling rebroadcast, since
		// rebroadcast timeout must have been reset now that progress is made.
		driver.RequireDeliverAlarm()
		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger rebroadcast alarm.
		driver.RequireDeliverAlarm()

		// Expect rebroadcast of all messages from previous and current round, including
		// QUALITY.
		driver.RequireQuality()
		driver.RequireCommit(1, baseChain, evidenceOfPrepareAtRound1)
		driver.RequirePrepareAtRound(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireConverge(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequirePrepare(baseChain)

		// Facilitate skip to future round to assert rebroadcast only contains messages
		// from the latest 2 rounds, plus QUALITY from round 0.

		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)
		evidenceOfPrepareAtRound76 := instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 0, 1)
		evidenceOfPrepareAtRound77 := instance.NewJustification(77, gpbft.PREPARE_PHASE, futureRoundProposal, 0, 1)

		// Send Prepare messages to facilitate weak quorum of prepare at future round.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(77, futureRoundProposal),
			Justification: evidenceOfPrepareAtRound76,
		})
		// Send Converge at future round to facilitate proposal with highest ticket.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: evidenceOfPrepareAtRound76,
			Ticket:        emulator.ValidTicket,
		})
		// Expect skip to round.
		driver.RequireConverge(77, futureRoundProposal, evidenceOfPrepareAtRound76)
		driver.RequirePrepareAtRound(77, futureRoundProposal, evidenceOfPrepareAtRound76)
		driver.RequireCommit(77, futureRoundProposal, evidenceOfPrepareAtRound77)
		// Expect no messages until the rebroadcast timeout has expired.
		driver.RequireNoBroadcast()
		// Trigger rebroadcast alarm.
		driver.RequireDeliverAlarm()

		// Assert that rebroadcast includes QUALITY and messages from round 77, but none
		// from round 1 since the node should only retain messages from the latest two
		// rounds, plus quality since it skiped round and has never seen round 76.
		//
		// See: https://github.com/filecoin-project/go-f3/issues/595
		driver.RequireQuality()
		driver.RequireCommit(77, futureRoundProposal, evidenceOfPrepareAtRound77)
		driver.RequirePrepareAtRound(77, futureRoundProposal, evidenceOfPrepareAtRound76)
		driver.RequireConverge(77, futureRoundProposal, evidenceOfPrepareAtRound76)
		driver.RequireNoBroadcast()

		// Deliver COMMIT at round 77 to facilitate progress to DECIDE.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(77, futureRoundProposal),
			Justification: evidenceOfPrepareAtRound77,
		})

		// Expect DECIDE with strong evidence of COMMIT.
		evidenceOfCommit := instance.NewJustification(77, gpbft.COMMIT_PHASE, futureRoundProposal, 0, 1)
		driver.RequireDecide(futureRoundProposal, evidenceOfCommit)

		// Trigger alarm and expect immediate rebroadcast, because DECIDE phase has no
		// phase timeout.
		driver.RequireDeliverAlarm()

		// Expect rebroadcast of only the DECIDE message.
		driver.RequireDecide(futureRoundProposal, evidenceOfCommit)

		// Deliver DECIDE to facilitate progress to decision.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, futureRoundProposal),
			Justification: evidenceOfCommit,
		})
		driver.RequireDecision(instance.ID(), futureRoundProposal)
	})

	t.Run("Rebroadcasts independent of phase timeout after 3 rounds", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		justification := instance.NewJustification(12, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        0,
			Vote:          instance.NewConverge(13, instance.Proposal()),
			Ticket:        emulator.ValidTicket,
			Justification: justification,
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        0,
			Vote:          instance.NewPrepare(13, instance.Proposal()),
			Ticket:        emulator.ValidTicket,
			Justification: justification,
		})

		requireRebroadcast := func() {
			driver.RequireQuality()
			driver.RequireConverge(13, instance.Proposal(), justification)
			driver.RequireNoBroadcast()
		}

		driver.RequireConverge(13, instance.Proposal(), justification)       // Should schedule rebroadcast.
		driver.RequireNoBroadcast()                                          // Assert no broadcast until timeout expires.
		driver.RequireDeliverAlarm()                                         // Expire the timeout.
		requireRebroadcast()                                                 // Assert rebroadcast, which should schedule the next rebroadcast.
		driver.RequireDeliverAlarm()                                         // Trigger alarm
		requireRebroadcast()                                                 // Expect rebroadcast again, because phase timeout should be enough in the future since we are in round 13.
		driver.AdvanceTimeBy(24 * time.Hour)                                 // Now, advance the clock beyond the rebroadcast timeout.
		driver.RequireDeliverAlarm()                                         // Trigger alarm, which should trigger the phase timeout instead of rebroadcast timeout.
		driver.RequirePrepareAtRound(13, instance.Proposal(), justification) // Expect progress to PREPARE phase; that is no rebroadcast.
		driver.RequireNoBroadcast()                                          // Expect no further broadcast because PREPARE phase is not timed out yet.

		// Now, because we are beyond round 3, we should expect rebroadcast even though the phase timeout
		// hasn't expired yet. This is because the rebroadcast is set to trigger immediately beyond round 3.
		// Therefore, assert that rebroadcast is triggered, and this time it includes a PREPARE message.
		driver.RequireDeliverAlarm()
		driver.RequireQuality()
		driver.RequirePrepareAtRound(13, instance.Proposal(), justification)
		driver.RequireConverge(13, instance.Proposal(), justification)
		driver.RequireNoBroadcast() // Nothing else should be broadcast until the next alarm.
	})

	t.Run("When in PREPARE at round 0", func(t *testing.T) {
		// Advances the instance to PREPARE at round zero.
		whenInPrepareAtRoundZero := func(t *testing.T, instance *emulator.Instance, driver *emulator.Driver) {
			driver.RequireStartInstance(instance.ID())
			driver.RequireQuality()
			driver.RequireNoBroadcast()
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewQuality(instance.Proposal()),
			})
			driver.RequireDeliverAlarm()
			driver.RequirePrepare(instance.Proposal())
			driver.RequireNoBroadcast()
		}
		t.Run("Justification of COMMIT completes phase", func(t *testing.T) {
			instance, driver := newInstanceAndDriver(t)
			whenInPrepareAtRoundZero(t, instance, driver)

			// At this point, sender 0 is in PREPARE phase for the instance proposal. Now,
			// send a COMMIT message carrying justification of PREPARE from all participants,
			// which should complete the PREPARE phase immediately even though sender 0
			// hasn't seen a strong quorum for the instance proposal via PREPARE messages.
			evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewCommit(0, instance.Proposal()),
				Justification: evidenceOfPrepare,
			})
			driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
		})
		t.Run("Justification of PREPARE from next round completes phase", func(t *testing.T) {
			instance, driver := newInstanceAndDriver(t)
			whenInPrepareAtRoundZero(t, instance, driver)

			// At this point, sender 0 is in PREPARE phase for the instance proposal. Now,
			// send a PREPARE message from round 1 carrying justification of PREPARE from all
			// participants in round 0, which should complete the PREPARE phase immediately
			// even though sender 0 hasn't seen a strong quorum for the instance proposal via
			// PREPARE messages.
			evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewPrepare(1, instance.Proposal()),
				Justification: evidenceOfPrepare,
			})
			driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
		})
		t.Run("Justification of CONVERGE from next round completes phase", func(t *testing.T) {
			instance, driver := newInstanceAndDriver(t)
			whenInPrepareAtRoundZero(t, instance, driver)

			// At this point, sender 0 is in PREPARE phase for the instance proposal. Now,
			// send a CONVERGE message from round 1 carrying justification of PREPARE from all
			// participants in round 0, which should complete the PREPARE phase immediately
			// even though sender 0 hasn't seen a strong quorum for the instance proposal via
			// PREPARE messages.
			evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewConverge(1, instance.Proposal()),
				Ticket:        emulator.ValidTicket,
				Justification: evidenceOfPrepare,
			})
			driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
		})
	})

	t.Run("When in COMMIT for base at round 0", func(t *testing.T) {
		// Advances the instance to COMMIT for base decision at round zero.
		whenInCommitForBaseAtRoundZero := func(t *testing.T, instance *emulator.Instance, driver *emulator.Driver) {
			driver.RequireStartInstance(instance.ID())
			driver.RequireQuality()
			driver.RequireNoBroadcast()
			// Timeout QUALITY phase immediately without delivering any QUALITY messages.
			driver.RequireDeliverAlarm()
			// Assert PREPARE phase for base decision and progress PREPARE to commit.
			driver.RequirePrepare(instance.Proposal().BaseChain())
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewPrepare(0, instance.Proposal().BaseChain()),
			})
			// Assert COMMIT phase for base decision.
			driver.RequireCommit(0, instance.Proposal().BaseChain(), instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal().BaseChain(), 0, 1))
		}
		t.Run("Justification of CONVERGE to bottom from next round completes phase", func(t *testing.T) {
			instance, driver := newInstanceAndDriver(t)
			whenInCommitForBaseAtRoundZero(t, instance, driver)

			// At this point, sender 0 is in the COMMIT phase for the instance proposal. Now,
			// send a CONVERGE message from the next round carrying justification as evidence
			// of COMMIT to the bottom, which should complete the COMMIT phase immediately.
			evidenceOfCommitToBottom := instance.NewJustification(0, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewConverge(1, instance.Proposal().BaseChain()),
				Ticket:        emulator.ValidTicket,
				Justification: evidenceOfCommitToBottom,
			})
			driver.RequireConverge(1, instance.Proposal().BaseChain(), evidenceOfCommitToBottom)
		})
		t.Run("Justification of PREPARE for bottom from next round completes phase", func(t *testing.T) {
			instance, driver := newInstanceAndDriver(t)
			whenInCommitForBaseAtRoundZero(t, instance, driver)

			// At this point, sender 0 is in the COMMIT phase for the instance proposal. Now,
			// send a PREPARE message from the next round carrying justification as evidence
			// of COMMIT to the bottom, which should complete the COMMIT phase immediately.
			evidenceOfCommitToBottom := instance.NewJustification(0, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewPrepare(1, instance.Proposal().BaseChain()),
				Justification: evidenceOfCommitToBottom,
			})
			driver.RequireConverge(1, instance.Proposal().BaseChain(), evidenceOfCommitToBottom)
		})
	})
}

func TestGPBFT_WithExactOneThirdToTwoThirdPowerDistribution(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(1),
				},
				gpbft.PowerEntry{
					ID:    1,
					Power: gpbft.NewStoragePower(2),
				},
			},
			tipset0, tipSet1, tipSet2, tipSet3, tipSet4,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Decides alternative proposal from participant with 2/3 of power", func(t *testing.T) {
		// Test that messages from participant with 2/3 of power are sufficient on their
		// own to reach a decision for that participant's proposal.

		instance, driver := newInstanceAndDriver(t)
		alternativeProposal := instance.Proposal().BaseChain().Extend(tipSet1.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(alternativeProposal),
		})
		// Participants wait until either there is a quorum *for their own proposal*,
		// i.e. instance proposal, or a timeout. Here, the only other QUALITY message
		// being delivered is for the alternative proposal. Hence, the timeout trigger to
		// end QUALITY and force progress to PREPARE.
		driver.RequireDeliverAlarm()

		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, alternativeProposal),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, alternativeProposal),
			Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, alternativeProposal, 1),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, alternativeProposal),
			Justification: instance.NewJustification(0, gpbft.COMMIT_PHASE, alternativeProposal, 1),
		})
		driver.RequireDecision(instance.ID(), alternativeProposal)
	})

	t.Run("Gets stuck if participant with 2/3 of power sends no messages", func(t *testing.T) {
		// Test that if only messages from participant with 1/3 of power are delivered
		// the instance gets stuck on PREPARE for base chain, rebroadcasting QUALITY and
		// PREPARE. After delivering PREPARE from the participant with 2/3 of power it
		// then gets stuck at COMMIT for base rebroadcasting QUALITY, PREPARE and COMMIT.
		// And finally after delivering COMMIT from participant with 2/3 of power it
		// decides on base justified by that participant's COMMIT.
		instance, driver := newInstanceAndDriver(t)
		baseChain := instance.Proposal().BaseChain()

		driver.RequireStartInstance(instance.ID())

		// Deliver QUALITY from participant with 1/3 of power.
		driver.RequireQuality()
		driver.RequireNoBroadcast()
		// Trigger timeout to force progress to PREPARE since without timeout there is no
		// strong quorum for instance proposal.
		driver.RequireDeliverAlarm()

		// Deliver PREPARE from participant with 1/3 of power.
		driver.RequirePrepare(baseChain)
		// Trigger timeout of PREPARE phase to force a scheduled re-broadcast.
		driver.RequireDeliverAlarm()

		// Trigger timeout of re-broadcast and expect QUALITY and PREPARE for base to be
		// re-broadcasted.
		driver.RequireDeliverAlarm()
		driver.RequireQuality()
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()

		// Unstuck the instance from PREPARE.
		driver.RequireDeliverMessage(
			&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewPrepare(0, baseChain),
			},
		)
		driver.RequireCommit(0, baseChain, instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 1))

		// Trigger timeout of COMMIT phase to force a scheduled re-broadcast.
		driver.RequireDeliverAlarm()

		// Trigger timeout of re-broadcast and expect QUALITY, PREPARE, and COMMIT for
		// base to be re-broadcasted.
		driver.RequireDeliverAlarm()
		driver.RequireQuality()
		driver.RequireCommit(0, baseChain, instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 1))
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()

		// Unstuck the instance from COMMIT.
		driver.RequireDeliverMessage(
			&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewCommit(0, baseChain),
				Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 1),
			},
		)

		// Assert that decision is reached with justification from participant with 1/3 of power
		driver.RequireDecide(baseChain, instance.NewJustification(0, gpbft.COMMIT_PHASE, baseChain, 1))
	})
}

func TestGPBFT_SkipsToDecide(t *testing.T) {
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(1),
				},
				gpbft.PowerEntry{
					ID: 1,
					// Set majority power.
					Power: gpbft.NewStoragePower(3),
				},
			},
			tipset0, tipSet1, tipSet2,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("After Instance started", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		wantDecision := instance.Proposal().Extend(tipSet4.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewDecide(0, wantDecision),
			// Justify by strong evidence of COMMIT from 1, the participant with majority power.
			Justification: instance.NewJustification(0, gpbft.COMMIT_PHASE, wantDecision, 1),
		})
		// Expect immediate decision.
		driver.RequireDecision(instance.ID(), wantDecision)
	})
	t.Run("Before instance started via dequeue messages", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		wantDecision := instance.Proposal().Extend(tipSet4.Key)

		// Send the DECIDE message before instance start to force it to be queued.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewDecide(0, wantDecision),
			// Justify by strong evidence of COMMIT from 1, the participant with majority power.
			Justification: instance.NewJustification(0, gpbft.COMMIT_PHASE, wantDecision, 1),
		})

		driver.RequireStartInstance(instance.ID())
		// Expect immediate decision.
		driver.RequireDecision(instance.ID(), wantDecision)
	})
}

func TestGPBFT_SoloParticipant(t *testing.T) {
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(1),
				},
			},
			tipset0, tipSet1, tipSet2,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Decides proposal with no timeout", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(instance.Proposal())
		driver.RequireCommit(
			0,
			instance.Proposal(),
			instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0),
		)
		driver.RequireDecide(
			instance.Proposal(),
			instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 0),
		)
		driver.RequireDecision(instance.ID(), instance.Proposal())
	})

	t.Run("Decides base on QUALITY timeout", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		baseChain := instance.Proposal().BaseChain()
		driver.RequireStartInstance(instance.ID())
		driver.RequireDeliverAlarm()
		driver.RequireQuality()
		driver.RequirePrepare(baseChain)
		driver.RequireCommit(
			0,
			baseChain,
			instance.NewJustification(0, gpbft.PREPARE_PHASE, baseChain, 0),
		)
		driver.RequireDecide(
			baseChain,
			instance.NewJustification(0, gpbft.COMMIT_PHASE, baseChain, 0),
		)
		driver.RequireDecision(instance.ID(), baseChain)
	})
}

func TestGPBFT_SkipsToRound(t *testing.T) {
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(1),
				},
				gpbft.PowerEntry{
					ID:    1,
					Power: gpbft.NewStoragePower(4),
				},
			},
			tipset0, tipSet1, tipSet2,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Will not skip round in Decide phase", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		// Advance driver to Decide phase by sending an early arriving Decide justified
		// by strong quorum of commits from participant IDs 1 and 2, making up 2/3 of the
		// power.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, instance.Proposal()),
			Justification: instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 1),
		})

		// At this point, the subject must be in Decide phase; therefore assert that any
		// further messages will fail due to decision at instance zero and progress to
		// next instance.
		//
		// Note, sending any message from instance 0 would fail. For clarity we send
		// explicit messages that otherwise would have caused skip to round.
		driver.RequireErrOnDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
		}, gpbft.ErrValidationTooOld, "")
		driver.RequireErrOnDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
			Ticket:        emulator.ValidTicket,
		}, gpbft.ErrValidationTooOld, "")
		// Expect decision.
		driver.RequireDecision(instance.ID(), instance.Proposal())
	})

	t.Run("Will not skip round on absence of weak quorum of prepare", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		// Send Converge at future round to facilitate proposal with highest ticket.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
			Ticket:        emulator.ValidTicket,
		})

		// Expect no broadcasts as the subject must still be in quality round.
		driver.RequireNoBroadcast()
		// Deliver an alarm, which must cause progress to prepare for instance proposal.
		driver.RequireDeliverAlarm()
		// Expect prepare for base chain at round zero to clearly assert that skip round has not occurred.
		driver.RequirePrepare(instance.Proposal().BaseChain())
	})

	t.Run("Will skip round on weak quorum of prepare and max ticket", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()

		// Send Prepare messages to facilitate weak quorum of prepare at future round.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
		})
		// Send Converge at future round to facilitate proposal with highest ticket.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
			Ticket:        emulator.ValidTicket,
		})
		// Expect skip to round.
		driver.RequireConverge(77, futureRoundProposal, instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1))
	})

	t.Run("Will skip round before start on weak quorum of prepare and max ticket", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)

		// Here we send the necessary messages before the instance start, which means
		// messages are queued first then drained once the instance starts. This
		// exercises a different path, where skip round occurs by draining the queued
		// messages.

		// Send Prepare messages to facilitate weak quorum of prepare at future round.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewPrepare(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
		})
		// Send Converge at future round to facilitate proposal with highest ticket.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
			Ticket:        emulator.ValidTicket,
		})

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()

		// Expect skip to round.
		driver.RequireConverge(77, futureRoundProposal, instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1))
	})
}

func TestGPBFT_Equivocations(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
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
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Decides on proposal at instance", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)

		equivocations := []*gpbft.ECChain{
			instance.Proposal().Extend(tipSet3.Key),
			instance.Proposal().Extend(tipSet4.Key),
		}

		driver.RequireStartInstance(instance.ID())

		// Send the first Quality message for instance proposal
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(instance.Proposal()),
		})

		// Equivocate in Quality round.
		for _, equivocation := range equivocations {
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewQuality(equivocation),
			})
		}

		// Then deliver the quality message from the participant to itself.
		// This must result in progress to Prepare.
		driver.RequireQuality()

		// Send prepare for instance proposal.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})

		// Equivocate in Prepare round.
		for _, equivocation := range equivocations {
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewPrepare(0, equivocation),
			})
		}
		driver.RequirePrepare(instance.Proposal())

		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, instance.Proposal()),
			Justification: evidenceOfPrepare,
		})

		// Equivocate in Commit round.
		for _, equivocation := range equivocations {
			equivocatingEvidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, equivocation, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewCommit(0, equivocation),
				Justification: equivocatingEvidenceOfPrepare,
			})
		}
		// Require commit with expected evidence of prepare for proposal despite equivocations.
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)

		evidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, instance.Proposal()),
			Justification: evidenceOfCommit,
		})

		// Equivocate in Decide round.
		for _, equivocation := range equivocations {
			equivocatingEvidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, equivocation, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewDecide(0, equivocation),
				Justification: equivocatingEvidenceOfCommit,
			})
		}
		// Expect decide with the right evidence regardless of equivocations.
		driver.RequireDecide(instance.Proposal(), evidenceOfCommit)

		// Expect decision on instance proposal.
		driver.RequireDecision(instance.ID(), instance.Proposal())
	})

	t.Run("With queued messages/Decides on proposal", func(t *testing.T) {

		// This test sends all the messages first, then asserts state transition.

		instance, driver := newInstanceAndDriver(t)

		equivocations := []*gpbft.ECChain{
			instance.Proposal().Extend(tipSet3.Key),
			instance.Proposal().Extend(tipSet4.Key),
		}

		// Send the first Quality message for instance proposal
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewQuality(instance.Proposal()),
		})

		// Equivocate in Quality round.
		for _, equivocation := range equivocations {
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewQuality(equivocation),
			})
		}

		// Send prepare for instance proposal.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})

		// Equivocate in Prepare round.
		for _, equivocation := range equivocations {
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender: 1,
				Vote:   instance.NewPrepare(0, equivocation),
			})
		}

		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, instance.Proposal()),
			Justification: evidenceOfPrepare,
		})
		for _, equivocation := range equivocations {
			equivocatingEvidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, equivocation, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewCommit(0, equivocation),
				Justification: equivocatingEvidenceOfPrepare,
			})
		}

		// Start the instance before sending decide messages to avoid skip to decide.
		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(instance.Proposal())
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)

		evidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1)
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, instance.Proposal()),
			Justification: evidenceOfCommit,
		})
		for _, equivocation := range equivocations {
			equivocatingEvidenceOfCommit := instance.NewJustification(0, gpbft.COMMIT_PHASE, equivocation, 0, 1)
			driver.RequireDeliverMessage(&gpbft.GMessage{
				Sender:        1,
				Vote:          instance.NewDecide(0, equivocation),
				Justification: equivocatingEvidenceOfCommit,
			})
		}
		driver.RequireDecide(instance.Proposal(), evidenceOfCommit)
		driver.RequireDecision(instance.ID(), instance.Proposal())
	})
}

func TestGPBFT_ImpossibleQuorum(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(1),
				},
				gpbft.PowerEntry{
					ID:    1,
					Power: gpbft.NewStoragePower(2),
				},
				gpbft.PowerEntry{
					ID:    2,
					Power: gpbft.NewStoragePower(1),
				},
			},
			tipset0, tipSet1, tipSet2,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	t.Run("Exits early from Prepare", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		alternativeProposal := instance.Proposal().BaseChain().Extend([]byte("barreleye"))

		driver.RequireStartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequireNoBroadcast()
		driver.RequireDeliverAlarm()
		driver.RequirePrepare(instance.Proposal().BaseChain())

		// Send Prepare for alternative proposal, which makes quorum impossible.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, alternativeProposal),
		})

		// Expect immediate progress to Commit without any further messages or alarm.
		driver.RequireCommitForBottom(0)
	})
}

func TestGPBFT_Validation(t *testing.T) {
	t.Parallel()
	participants := gpbft.PowerEntries{
		gpbft.PowerEntry{
			ID:    0,
			Power: gpbft.NewStoragePower(1),
		},
		gpbft.PowerEntry{
			ID:    1,
			Power: gpbft.NewStoragePower(1),
		},
		gpbft.PowerEntry{
			ID:    2,
			Power: gpbft.NewStoragePower(3),
		},
	}

	tests := []struct {
		name        string
		message     func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage
		errContains string
	}{
		{
			name: "Decide justified by Commit with minority power",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewDecide(0, instance.Proposal()),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(77, gpbft.COMMIT_PHASE, instance.Proposal(), 1),
				}
			},
			errContains: "insufficient power",
		},
		{
			name: "Invalid Chain",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote: instance.NewQuality(&gpbft.ECChain{TipSets: []*gpbft.TipSet{{
						Epoch:      -1,
						PowerTable: instance.SupplementalData().PowerTable,
					}}}),
				}
			},
			errContains: "invalid message vote value",
		},
		{
			name: "inconsistent supp data",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote: gpbft.Payload{
						Phase: gpbft.DECIDE_PHASE,
						SupplementalData: gpbft.SupplementalData{
							PowerTable: gpbft.MakeCid([]byte("fish")),
						},
						Value: instance.Proposal(),
					},
					Justification: instance.NewJustification(55, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1),
				}
			},
			errContains: "inconsistent supplemental data",
		},
		{
			name: "justification for different value",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote: gpbft.Payload{
						Phase:            gpbft.DECIDE_PHASE,
						Value:            instance.Proposal(),
						SupplementalData: instance.SupplementalData(),
					},
					Justification: instance.NewJustification(55, gpbft.COMMIT_PHASE, instance.Proposal().Extend([]byte("lobster")), 0, 1),
				}
			},
			errContains: "justification for a different value",
		},
		{
			name: "justification with invalid value",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote: gpbft.Payload{
						Phase:            gpbft.DECIDE_PHASE,
						Value:            instance.Proposal(),
						SupplementalData: instance.SupplementalData(),
					},
					Justification: instance.NewJustification(55, gpbft.COMMIT_PHASE, &gpbft.ECChain{TipSets: []*gpbft.TipSet{{
						Epoch:      -2,
						PowerTable: instance.SupplementalData().PowerTable,
					}}}, 0, 1),
				}
			},
			errContains: "invalid justification vote value",
		},
		{
			name: "justification for different instance",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				newInstance := emulator.NewInstance(t, instance.ID()+1, instance.PowerTable().Entries, instance.Proposal().TipSets...)
				return &gpbft.GMessage{
					Sender: 1,
					Vote: gpbft.Payload{
						Phase:            gpbft.DECIDE_PHASE,
						Value:            instance.Proposal(),
						SupplementalData: instance.SupplementalData(),
					},
					Justification: newInstance.NewJustification(55, gpbft.COMMIT_PHASE, instance.Proposal().Extend([]byte("lobster")), 0, 1),
				}
			},
			errContains: "evidence from instanceID",
		},
		{
			name: "missing justification",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewConverge(12, instance.Proposal()),
					Ticket: emulator.ValidTicket,
				}
			},
			errContains: "has no justification",
		},
		{
			name: "converge for bottom",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewConverge(12, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(11, gpbft.PREPARE_PHASE, &gpbft.ECChain{}, 0, 1),
				}
			},
			errContains: "unexpected zero value for converge phase",
		},
		{
			name: "non-zero value for PREPARE justified by COMMIT",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewPrepare(3, instance.Proposal()),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(2, gpbft.COMMIT_PHASE, instance.Proposal(), 0, 1),
				}
			},
			errContains: "justification for a different value",
		},
		{
			name: "unexpected phase for justification of PREPARE",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewPrepare(3, instance.Proposal()),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(2, gpbft.CONVERGE_PHASE, instance.Proposal(), 0, 1),
				}
			},
			errContains: "justification with unexpected phase",
		},
		{
			name: "justification without strong quorum",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewPrepare(3, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(2, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0),
				}
			},
			errContains: "has justification with insufficient power",
		},
		{
			name: "justification with unknown signer",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				otherInstance := emulator.NewInstance(t, 0, append(participants, gpbft.PowerEntry{
					ID:    42,
					Power: gpbft.NewStoragePower(1),
				}), tipset0, tipSet1, tipSet2)

				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewPrepare(3, &gpbft.ECChain{}),
					Ticket: emulator.ValidTicket,
					Justification: otherInstance.NewJustificationWithPayload(gpbft.Payload{
						Instance:         instance.ID(),
						Round:            2,
						Phase:            gpbft.COMMIT_PHASE,
						SupplementalData: instance.SupplementalData(),
						Value:            &gpbft.ECChain{},
					}, 0, 1, 2, 42),
				}
			},
			errContains: "invalid signer index: 3",
		},
		{
			name: "justification for another instance",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				otherInstance := emulator.NewInstance(t, 33, participants, instance.Proposal().TipSets...)
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewPrepare(3, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: otherInstance.NewJustification(0, gpbft.PREPARE_PHASE, &gpbft.ECChain{}, 0, 1, 2),
				}
			},
			errContains: "message with instanceID 0 has evidence from instanceID: 33",
		},
		{
			name: "justification with inconsistent supplemental data",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				someCid, err := cid.Decode("bafy2bzacedgbq7e3eb4l3ryhbfckvez5mty6ylw5ulofkxql5bfxxji7xt5a2")
				require.NoError(t, err)
				require.NotEqual(t, instance.SupplementalData().PowerTable, someCid)
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewPrepare(3, &gpbft.ECChain{}),
					Ticket: emulator.ValidTicket,
					Justification: instance.NewJustificationWithPayload(gpbft.Payload{
						Instance: instance.ID(),
						Round:    2,
						Phase:    gpbft.COMMIT_PHASE,
						SupplementalData: gpbft.SupplementalData{
							Commitments: [32]byte{},
							PowerTable:  someCid,
						},
						Value: &gpbft.ECChain{},
					}, 0, 1, 2),
				}
			},
			errContains: "message and justification have inconsistent supplemental data",
		},
		{
			name: "justification aggregate with unsorted signatures",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewPrepare(3, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(2, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0, 1, 2),
				}
			},
			errContains: "invalid aggregate",
		},
		{
			name: "out of order epochs",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				proposal := instance.Proposal()
				proposal.TipSets[1], proposal.TipSets[2] = proposal.TipSets[2], proposal.TipSets[1]
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(proposal),
				}
			},
			errContains: "chain must have increasing epochs",
		},
		{
			name: "repeated epochs",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				proposal := instance.Proposal()
				proposal.TipSets[1] = proposal.TipSets[2]
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(proposal),
				}
			},
			errContains: "chain must have increasing epochs",
		},
		{
			name: "QUALITY with empty value",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(&gpbft.ECChain{}),
				}
			},
			errContains: "unexpected zero value for quality phase",
		},
		{
			name: "CONVERGE with empty value justified by strong quorum of PREPARE for bottom",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewConverge(1, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, &gpbft.ECChain{}, 0, 1),
				}
			},
			errContains: "unexpected zero value for converge phase",
		},
		{
			name: "DECIDE with empty value justified by strong quorum of COMMIT for bottom",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewDecide(0, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(0, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0, 1),
				}
			},
			errContains: "unexpected zero value for decide phase",
		},
		{
			name: "DECIDE for non-zero round",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender:        1,
					Vote:          instance.NewDecide(7, &gpbft.ECChain{}),
					Ticket:        emulator.ValidTicket,
					Justification: instance.NewJustification(6, gpbft.COMMIT_PHASE, &gpbft.ECChain{}, 0, 1),
				}
			},
			errContains: "unexpected non-zero round 7 for decide phase",
		},
		{
			name: "QUALITY with too long a chain",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				powerTableCid := instance.Proposal().TipSets[0].PowerTable
				commitments := instance.Proposal().TipSets[0].Commitments
				tooLongAChain := &gpbft.ECChain{
					TipSets: make([]*gpbft.TipSet, gpbft.ChainMaxLen+1),
				}
				for i := range tooLongAChain.TipSets {
					tooLongAChain.TipSets[i] = &gpbft.TipSet{
						Epoch:       int64(i + 1),
						Key:         nil,
						PowerTable:  powerTableCid,
						Commitments: commitments,
					}
				}
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(tooLongAChain),
				}
			},
			errContains: "chain too long",
		},
		{
			name: "QUALITY with too long a tipset key",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				var tooLongATipsetKey bytes.Buffer
				_, err := tooLongATipsetKey.ReadFrom(io.LimitReader(rand.Reader, gpbft.TipsetKeyMaxLen+1))
				require.NoError(t, err)
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(instance.Proposal().Extend(tooLongATipsetKey.Bytes())),
				}
			},
			errContains: "tipset key too long",
		},
		{
			name: "QUALITY with too long a tipset power table CID",
			message: func(instance *emulator.Instance, driver *emulator.Driver) *gpbft.GMessage {
				var tooLongACid bytes.Buffer
				_, err := tooLongACid.ReadFrom(io.LimitReader(rand.Reader, gpbft.CidMaxLen+1))
				require.NoError(t, err)
				proposal := instance.Proposal()
				proposal.TipSets[1] = &gpbft.TipSet{
					Epoch:      proposal.TipSets[1].Epoch,
					Key:        proposal.TipSets[1].Key,
					PowerTable: cid.NewCidV1(cid.Raw, tooLongACid.Bytes()),
				}
				return &gpbft.GMessage{
					Sender: 1,
					Vote:   instance.NewQuality(proposal),
				}
			},
			errContains: "power table CID too long",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			driver := emulator.NewDriver(t)
			instance := emulator.NewInstance(t, 0, participants, tipset0, tipSet1, tipSet2)
			driver.AddInstance(instance)
			driver.RequireNoBroadcast()
			message := test.message(instance, driver)
			driver.RequireErrOnDeliverMessage(message, gpbft.ErrValidationInvalid, test.errContains)
		})
	}
}

func TestGPBFT_DropOld(t *testing.T) {
	t.Parallel()
	participants := gpbft.PowerEntries{
		gpbft.PowerEntry{
			ID:    0,
			Power: gpbft.NewStoragePower(1),
		},
		gpbft.PowerEntry{
			ID:    1,
			Power: gpbft.NewStoragePower(1),
		},
	}

	// We define 3 instances, old, last, and new.

	driver := emulator.NewDriver(t)
	oldInstance := emulator.NewInstance(t, 0, participants, tipset0, tipSet1)
	lastInstance := emulator.NewInstance(t, 1, participants, tipSet1, tipSet2)
	newInstance := emulator.NewInstance(t, 2, participants, tipSet2)
	driver.AddInstance(oldInstance)
	driver.AddInstance(lastInstance)
	driver.AddInstance(newInstance)

	// We immediately skip to the "new" instance.
	driver.RequireStartInstance(2)

	// All messages from the old instance should be dropped.

	oldDecide := &gpbft.GMessage{
		Sender: 1,
		Vote:   oldInstance.NewDecide(0, oldInstance.Proposal()),
		Ticket: emulator.ValidTicket,
	}
	driver.RequireErrOnDeliverMessage(oldDecide, gpbft.ErrValidationTooOld, "message is for prior instance")

	// Everything except decides from the last instance should be dropped.

	lastCommit := &gpbft.GMessage{
		Sender: 1,
		Vote:   lastInstance.NewCommit(3, lastInstance.Proposal()),
		Ticket: emulator.ValidTicket,
	}
	driver.RequireErrOnDeliverMessage(lastCommit, gpbft.ErrValidationTooOld, "message is for prior instance")

	lastDecide := &gpbft.GMessage{
		Sender:        1,
		Vote:          lastInstance.NewDecide(0, lastInstance.Proposal()),
		Justification: lastInstance.NewJustification(0, gpbft.COMMIT_PHASE, lastInstance.Proposal(), 0, 1),
		Ticket:        emulator.ValidTicket,
	}
	driver.RequireDeliverMessage(lastDecide)

	// Everything should be delivered for the new instance.

	newQuality := &gpbft.GMessage{
		Sender: 1,
		Vote:   newInstance.NewQuality(newInstance.Proposal()),
		Ticket: emulator.ValidTicket,
	}
	newCommit0 := &gpbft.GMessage{
		Sender:        0,
		Vote:          newInstance.NewCommit(0, newInstance.Proposal()),
		Justification: newInstance.NewJustification(0, gpbft.PREPARE_PHASE, newInstance.Proposal(), 0, 1),
		Ticket:        emulator.ValidTicket,
	}
	newCommit1 := &gpbft.GMessage{
		Sender:        1,
		Vote:          newInstance.NewCommit(0, newInstance.Proposal()),
		Justification: newInstance.NewJustification(0, gpbft.PREPARE_PHASE, newInstance.Proposal(), 0, 1),
		Ticket:        emulator.ValidTicket,
	}
	newDecide0 := &gpbft.GMessage{
		Sender: 0,
		Vote:   newInstance.NewDecide(0, newInstance.Proposal()),

		Justification: newInstance.NewJustification(0, gpbft.COMMIT_PHASE, newInstance.Proposal(), 0, 1),
		Ticket:        emulator.ValidTicket,
	}
	newDecide1 := &gpbft.GMessage{
		Sender: 1,
		Vote:   newInstance.NewDecide(0, newInstance.Proposal()),

		Justification: newInstance.NewJustification(0, gpbft.COMMIT_PHASE, newInstance.Proposal(), 0, 1),
		Ticket:        emulator.ValidTicket,
	}
	driver.RequireDeliverMessage(newQuality)
	driver.RequireDeliverMessage(newDecide0)
	// No quorum of decides, should still accept it but be considered not relevant
	driver.RequireErrOnDeliverMessage(newCommit0, gpbft.ErrValidationNotRelevant, "not relevant")
	driver.RequireDeliverMessage(newDecide1)

	// Once we've received two decides, we should reject messages from the "new" instance.

	driver.RequireErrOnDeliverMessage(newCommit1, gpbft.ErrValidationTooOld, "message is for prior instance")

	// And we should now reject decides from the "last" instance.
	driver.RequireErrOnDeliverMessage(lastDecide, gpbft.ErrValidationTooOld, "message is for prior instance")

	// But we should still accept decides from the latest instance.
	driver.RequireDeliverMessage(newDecide0)
}

func TestGPBFT_Sway(t *testing.T) {
	t.Parallel()
	newInstanceAndDriver := func(t *testing.T) (*emulator.Instance, *emulator.Driver) {
		driver := emulator.NewDriver(t)
		instance := emulator.NewInstance(t,
			0,
			gpbft.PowerEntries{
				gpbft.PowerEntry{
					ID:    0,
					Power: gpbft.NewStoragePower(2),
				},
				gpbft.PowerEntry{
					ID:    1,
					Power: gpbft.NewStoragePower(2),
				},
				gpbft.PowerEntry{
					ID:    2,
					Power: gpbft.NewStoragePower(1),
				},
				gpbft.PowerEntry{
					ID:    3,
					Power: gpbft.NewStoragePower(2),
				},
			},
			tipset0, tipSet1, tipSet2, tipSet3, tipSet4,
		)
		driver.AddInstance(instance)
		driver.RequireNoBroadcast()
		return instance, driver
	}

	swayed := func(t *testing.T) bool {
		instance, driver := newInstanceAndDriver(t)
		driver.RequireStartInstance(instance.ID())
		baseProposal := instance.Proposal().BaseChain()
		proposal2 := baseProposal.Extend(tipSet1.Key)
		proposal1 := proposal2.Extend(tipSet2.Key).Extend(tipSet3.Key)

		// Trigger alarm to immediately complete QUALITY.
		driver.RequireDeliverAlarm()
		driver.RequirePeekAtLastVote(gpbft.PREPARE_PHASE, 0, baseProposal)

		// Deliver PREPARE messages such that reaching quorum is impossible which should
		// complete the phase.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 0,
			Vote:   instance.NewPrepare(0, instance.Proposal()),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, proposal1),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 2,
			Vote:   instance.NewPrepare(0, proposal2),
		})
		driver.RequirePeekAtLastVote(gpbft.COMMIT_PHASE, 0, &gpbft.ECChain{})

		// Deliver COMMIT messages and trigger timeout to complete the phase but with no
		// strong quorum. This should progress the instance to CONVERGE at round 1.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(0, proposal2),
			Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, proposal2, 1, 3, 2),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 0,
			Vote:   instance.NewCommit(0, &gpbft.ECChain{}),
		})
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        3,
			Vote:          instance.NewCommit(0, proposal1),
			Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, proposal1, 1, 3, 2),
		})
		driver.RequireDeliverAlarm()

		// Assert sway from base to either proposal1 or proposal2 at COMMIT.
		latestBroadcast := driver.PeekLastBroadcastRequest()
		require.Equal(t, gpbft.CONVERGE_PHASE, latestBroadcast.Vote.Phase)
		require.EqualValues(t, 1, latestBroadcast.Vote.Round)
		swayedToProposal2AtCommit := latestBroadcast.Vote.Value.Eq(proposal2)
		require.True(t, latestBroadcast.Vote.Value.Eq(proposal1) || swayedToProposal2AtCommit)
		if !swayedToProposal2AtCommit {
			// Only proceed if swayed to proposal2 at COMMIT, so that we can test sway to
			// proposal1 at CONVERGE as an unseen proposal. Otherwise, it is possible that
			// CONVERGE may stick with proposal2, i.e. an already seen candidate.
			return false
		}

		// Deliver converge message for alternative proposal with strong quorum of
		// PREPARE, which should sway the subject. Then trigger alarm to end CONVERGE.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        3,
			Vote:          instance.NewConverge(1, proposal1),
			Ticket:        emulator.ValidTicket,
			Justification: instance.NewJustification(0, gpbft.PREPARE_PHASE, proposal1, 0, 1, 3, 2),
		})
		driver.RequireDeliverAlarm()

		// Only pass if CONVERGE swayed to either proposal1 or proposal2 but not the same
		// as whichever COMMIT swayed to.
		latestBroadcast = driver.PeekLastBroadcastRequest()
		require.Equal(t, gpbft.PREPARE_PHASE, latestBroadcast.Vote.Phase)
		require.EqualValues(t, 1, latestBroadcast.Vote.Round)
		swayedToProposal1AtConverge := latestBroadcast.Vote.Value.Eq(proposal1)
		require.True(t, swayedToProposal1AtConverge || latestBroadcast.Vote.Value.Eq(proposal2))
		return swayedToProposal1AtConverge
	}

	// Swaying is nondeterministic. Require that eventually we sway to one proposal in
	// COMMIT and other in CONVERGE.
	t.Run("Sways to alternative unseen proposal at COMMIT and CONVERGE", func(t *testing.T) {
		// Try 10 times to hit the exact sway we want.
		for i := 0; i < 10; i++ {
			if swayed(t) {
				return
			}
		}
		require.Fail(t, "after 10 tries did not swayed to proposals 1 and 2 at CONVERGE and COMMIT, respectively.")
	})
}

func TestSupplementalDataSerialization(t *testing.T) {
	t.Parallel()
	var (
		testCases = []gpbft.SupplementalData{
			{
				PowerTable:  gpbft.MakeCid([]byte("fish")),
				Commitments: [32]byte{0x01},
			},
			{
				PowerTable:  gpbft.MakeCid([]byte("lobster")),
				Commitments: [32]byte{0x02},
			},
		}
	)

	t.Run("cbor round trip", func(t *testing.T) {
		req := require.New(t)
		for _, ts := range testCases {
			var buf bytes.Buffer
			req.NoError(ts.MarshalCBOR(&buf))
			t.Logf("cbor: %x", buf.Bytes())
			var rt gpbft.SupplementalData
			req.NoError(rt.UnmarshalCBOR(&buf))
			req.Equal(ts, rt)
		}
	})

	t.Run("json round trip", func(t *testing.T) {
		req := require.New(t)
		for _, ts := range testCases {
			data, err := ts.MarshalJSON()
			req.NoError(err)
			t.Logf("json: %s", data)
			var rt gpbft.SupplementalData
			req.NoError(rt.UnmarshalJSON(data))
			req.Equal(ts, rt)

			// check that the supplemental data is a base64 string
			var bareMap map[string]any
			req.NoError(json.Unmarshal(data, &bareMap))
			commitField, ok := bareMap["Commitments"].(string)
			req.True(ok)
			req.Len(commitField, 44)
		}
	})

	t.Run("json error cases", func(t *testing.T) {
		req := require.New(t)
		var ts gpbft.SupplementalData
		err := ts.UnmarshalJSON([]byte(`{"Commitments":"bm9wZQ==","PowerTable":{"/":"bafy2bzaced5zqzzbxzyzuq2tcxhuclnvdn3y6ijhurgaapnbayul2dd5gspc4"}}`))
		req.ErrorContains(err, "32 bytes")
	})
}
