package gpbft_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/emulator"
	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	tipset0 = gpbft.TipSet{Epoch: 0, Key: []byte("bigbang")}
	tipSet1 = gpbft.TipSet{Epoch: 1, Key: []byte("fish")}
	tipSet2 = gpbft.TipSet{Epoch: 2, Key: []byte("lobster")}
	tipSet3 = gpbft.TipSet{Epoch: 3, Key: []byte("fisherman")}
	tipSet4 = gpbft.TipSet{Epoch: 4, Key: []byte("lobstermucher")}
)

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
		driver.StartInstance(instance.ID())
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
		driver.StartInstance(instance.ID())
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
		driver.StartInstance(instance.ID())
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
			Vote:   instance.NewCommit(0, gpbft.ECChain{}),
		})

		evidenceOfCommitForBottom := instance.NewJustification(0, gpbft.COMMIT_PHASE, gpbft.ECChain{}, 0, 1)

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

		driver.StartInstance(instance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(instance.Proposal())
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
		driver.RequireCommit(0, instance.Proposal(), evidenceOfPrepare)
	})

	t.Run("Queues future instance messages during current instance", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureInstance := emulator.NewInstance(t,
			42,
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
		driver.StartInstance(instance.ID())
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
		driver.StartInstance(futureInstance.ID())
		driver.RequireQuality()
		driver.RequirePrepare(futureInstance.Proposal())
		driver.RequireCommit(0, futureInstance.Proposal(), instance.NewJustification(0, gpbft.PREPARE_PHASE, futureInstance.Proposal(), 0, 1))
	})

	t.Run("Rebroadcasts selected messages on timeout", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		driver.StartInstance(instance.ID())
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
		// Expect rebroadcast of PREPARE only; no QUALITY message should be rebroadcasted.
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()
		// Trigger alarm and expect immediate rebroadcast of PREMARE.
		driver.RequireDeliverAlarm()
		// Expect rebroadcast of PREPARE only; no QUALITY message should be rebroadcasted.
		driver.RequirePrepare(baseChain)
		driver.RequireNoBroadcast()

		// Deliver PREPARE for bottom to facilitate progress to COMMIT.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewPrepare(0, baseChain),
		})

		// Expect progress to COMMIT with strong evidence of PREPARE.
		evidenceOfPrepare := instance.NewJustification(0, gpbft.PREPARE_PHASE, instance.Proposal(), 0, 1)
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

		// Expect rebroadcast of PREPARE and COMMIT.
		driver.RequirePrepare(baseChain)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequireNoBroadcast()

		// Deliver COMMIT for bottom to facilitate progress to CONVERGE.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender: 1,
			Vote:   instance.NewCommit(0, gpbft.ECChain{}),
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

		// Expect rebroadcast of all messages from previous and current round (except QUALITY)
		driver.RequirePrepare(baseChain)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequireConverge(1, baseChain, evidenceOfPrepareForBase)
		driver.RequirePrepareAtRound(1, baseChain, evidenceOfPrepareForBase)

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

		// Expect rebroadcast of all messages from previous and current round, except
		// QUALITY.
		driver.RequirePrepare(baseChain)
		driver.RequireCommit(0, baseChain, evidenceOfPrepare)
		driver.RequireConverge(1, baseChain, evidenceOfPrepareForBase)
		driver.RequirePrepareAtRound(1, baseChain, evidenceOfPrepareForBase)
		driver.RequireCommit(1, baseChain, evidenceOfPrepareAtRound1)

		// Deliver COMMIT at round 1 to facilitate progress to DECIDE.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewCommit(1, baseChain),
			Justification: evidenceOfPrepareAtRound1,
		})

		// Expect DECIDE with strong evidence of COMMIT.
		evidenceOfCommit := instance.NewJustification(1, gpbft.COMMIT_PHASE, baseChain, 0, 1)
		driver.RequireDecide(baseChain, evidenceOfCommit)

		// Trigger alarm and expect immediate rebroadcast, because DECIDE phase has no
		// phase timeout.
		driver.RequireDeliverAlarm()

		// Expect rebroadcast of only the DECIDE message.
		driver.RequireDecide(baseChain, evidenceOfCommit)

		// Deliver DECIDE to facilitate progress to decision.
		driver.RequireDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewDecide(0, baseChain),
			Justification: evidenceOfCommit,
		})
		driver.RequireDecision(instance.ID(), baseChain)
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

		driver.StartInstance(instance.ID())
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

		driver.StartInstance(instance.ID())
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
		driver.StartInstance(instance.ID())
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
		driver.StartInstance(instance.ID())
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

		driver.StartInstance(instance.ID())
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
		}, gpbft.ErrValidationTooOld)
		driver.RequireErrOnDeliverMessage(&gpbft.GMessage{
			Sender:        1,
			Vote:          instance.NewConverge(77, futureRoundProposal),
			Justification: instance.NewJustification(76, gpbft.PREPARE_PHASE, futureRoundProposal, 1),
			Ticket:        emulator.ValidTicket,
		}, gpbft.ErrValidationTooOld)
		// Expect decision.
		driver.RequireDecision(instance.ID(), instance.Proposal())
	})

	t.Run("Will not skip round on absence of weak quorum of prepare", func(t *testing.T) {
		instance, driver := newInstanceAndDriver(t)
		futureRoundProposal := instance.Proposal().Extend(tipSet4.Key)

		driver.StartInstance(instance.ID())
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

		driver.StartInstance(instance.ID())
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

		driver.StartInstance(instance.ID())
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

		equivocations := []gpbft.ECChain{
			instance.Proposal().Extend(tipSet3.Key),
			instance.Proposal().Extend(tipSet4.Key),
		}

		driver.StartInstance(instance.ID())

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

		equivocations := []gpbft.ECChain{
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
		driver.StartInstance(instance.ID())
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

		driver.StartInstance(instance.ID())
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
