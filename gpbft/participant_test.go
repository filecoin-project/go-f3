package gpbft_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var somePowerEntry = gpbft.PowerEntry{
	ID:     1513,
	Power:  gpbft.NewStoragePower(1514),
	PubKey: gpbft.PubKey("ghoti"),
}

type participantTestSubject struct {
	*gpbft.Participant

	t    *testing.T
	rng  *rand.Rand
	host *gpbft.MockHost

	id               gpbft.ActorID
	pubKey           gpbft.PubKey
	instance         uint64
	networkName      gpbft.NetworkName
	canonicalChain   gpbft.ECChain
	supplementalData *gpbft.SupplementalData
	powerTable       *gpbft.PowerTable
	beacon           []byte
	time             time.Time
	delta            time.Duration
	trace            []string
}

func newParticipantTestSubject(t *testing.T, seed int64, instance uint64) *participantTestSubject {
	// Generate some canonical chain.
	canonicalChain, err := gpbft.NewChain(gpbft.TipSet{Epoch: 0, Key: []byte("genesis"), PowerTable: []byte("pt")})
	require.NoError(t, err)

	const (
		delta                = 2 * time.Second
		deltaBackOffExponent = 1.3
	)

	rng := rand.New(rand.NewSource(seed))
	subject := &participantTestSubject{
		t:              t,
		rng:            rng,
		id:             gpbft.ActorID(rng.Uint64()),
		pubKey:         generateRandomBytes(rng),
		delta:          delta,
		instance:       instance,
		networkName:    "fish",
		canonicalChain: canonicalChain,
		supplementalData: &gpbft.SupplementalData{
			Commitments: [32]byte{},
			PowerTable:  []byte("powertable"),
		},
		powerTable: gpbft.NewPowerTable(),
		beacon:     generateRandomBytes(rng),
		time:       time.Now(),
	}

	// Assure power table contains the power entry for the test subject
	require.NoError(t, subject.powerTable.Add(gpbft.PowerEntry{
		ID:     subject.id,
		Power:  gpbft.NewStoragePower(int64(rng.Intn(1413))),
		PubKey: subject.pubKey,
	}))

	subject.host = gpbft.NewMockHost(t)
	subject.Participant, err = gpbft.NewParticipant(subject.host,
		gpbft.WithTracer(subject),
		gpbft.WithDelta(delta),
		gpbft.WithDeltaBackOffExponent(deltaBackOffExponent))
	require.NoError(t, err)
	subject.requireNotStarted()
	return subject
}

func (pt *participantTestSubject) Log(format string, args ...any) {
	pt.trace = append(pt.trace, fmt.Sprintf(format, args...))
}

func (pt *participantTestSubject) expectBeginInstance() {
	// Prepare the test host.
	pt.host.On("GetProposalForInstance", pt.instance).Return(pt.supplementalData, pt.canonicalChain, nil)
	pt.host.On("GetCommitteeForInstance", pt.instance).Return(pt.powerTable, pt.beacon, nil).Once()
	pt.host.On("Time").Return(pt.time)
	pt.host.On("NetworkName").Return(pt.networkName).Maybe()
	// We need to use `Maybe` here because `MarshalPayloadForSigning` may be called
	// an additional time for verification.
	// Without the `Maybe` the tests immediately fails here:
	// https://github.com/filecoin-project/go-f3/blob/d27d281109d31485fc4ac103e2af58afb86c158f/gpbft/gpbft.go#L395
	pt.host.On("MarshalPayloadForSigning", pt.networkName, mock.AnythingOfType("*gpbft.Payload")).
		Return([]byte(gpbft.DOMAIN_SEPARATION_TAG + ":" + pt.networkName)).Maybe()

	// Expect calls to get the host state prior to beginning of an instance.
	pt.host.EXPECT().GetProposalForInstance(pt.instance)
	pt.host.EXPECT().GetCommitteeForInstance(pt.instance)
	pt.host.EXPECT().Time()

	// Expect alarm is set to 2X of configured delta.
	pt.host.EXPECT().SetAlarm(pt.time.Add(2 * pt.delta))

	// Expect a broadcast occurs with quality phase message, and the expected chain, signature.
	payload := gpbft.Payload{
		Instance:         pt.instance,
		Step:             gpbft.QUALITY_PHASE,
		Value:            pt.canonicalChain,
		SupplementalData: *pt.supplementalData,
	}
	pt.host.EXPECT().RequestBroadcast(mock.MatchedBy(func(o *gpbft.MessageBuilder) bool {
		return o.NetworkName == pt.networkName &&
			payload.Eq(&o.Payload) &&
			o.Justification == nil &&
			o.BeaconForTicket == nil
	})).Return(nil)
}

func (pt *participantTestSubject) requireNotStarted() {
	pt.t.Helper()
	require.Zero(pt.t, pt.CurrentRound())
	require.Equal(pt.t, "nil", pt.Describe())
}

func (pt *participantTestSubject) requireInstanceRoundPhase(wantInstance, wantRound uint64, wantPhase gpbft.Phase) {
	pt.t.Helper()
	require.Equal(pt.t, fmt.Sprintf("{%d}, round %d, phase %s", wantInstance, wantRound, wantPhase), pt.Describe())
}

func (pt *participantTestSubject) requireStart() {
	pt.expectBeginInstance()
	require.NoError(pt.t, pt.Start())
	pt.assertHostExpectations()
	pt.requireInstanceRoundPhase(pt.instance, 0, gpbft.QUALITY_PHASE)
}

// Participants start new instances asynchronously by setting an alarm
// that is triggered immediately, thus the underlying use of
// ReceiveAlarm() in tests that require starting instances.
// See [participant.go:Start()] for reference
func (pt *participantTestSubject) Start() error {
	pt.host.EXPECT().SetAlarm(pt.time)
	require.NoError(pt.t, pt.Participant.StartInstanceAt(pt.instance, pt.time))
	return pt.ReceiveAlarm()
}

func (pt *participantTestSubject) assertHostExpectations() bool {
	return pt.host.AssertExpectations(pt.t)
}

func (pt *participantTestSubject) mockValidSignature(target gpbft.PubKey, sig []byte) *mock.Call {
	return pt.host.
		On("Verify", target, pt.matchMessageSigningPayload(), sig).
		Return(nil)
}

func (pt *participantTestSubject) mockInvalidSignature(target gpbft.PubKey, sig []byte) {
	pt.host.On("Verify", target, pt.matchMessageSigningPayload(), sig).
		Return(errors.New("mock verification failure"))
}

func (pt *participantTestSubject) mockInvalidTicket(target gpbft.PubKey, ticket gpbft.Ticket) *mock.Call {
	return pt.host.On(
		"Verify",
		target,
		mock.MatchedBy(func(msg []byte) bool {
			return bytes.HasPrefix(msg, []byte(gpbft.DOMAIN_SEPARATION_TAG_VRF+":"+pt.networkName))
		}), []byte(ticket)).
		Return(errors.New("mock verification failure"))
}

func (pt *participantTestSubject) mockValidTicket(target gpbft.PubKey, ticket gpbft.Ticket) *mock.Call {
	return pt.host.On(
		"Verify",
		target,
		pt.matchTicketSigningPayload(), []byte(ticket)).
		Return(nil)
}

func (pt *participantTestSubject) mockCommitteeForInstance(instance uint64, powerTable *gpbft.PowerTable, beacon []byte) {
	pt.host.On("GetCommitteeForInstance", instance).Return(powerTable, beacon, nil).Once()
}

func (pt *participantTestSubject) mockCommitteeUnavailableForInstance(instance uint64) {
	pt.host.On("GetCommitteeForInstance", instance).Return(nil, nil, errors.New("committee not available"))
}

func (pt *participantTestSubject) matchMessageSigningPayload() any {
	return mock.MatchedBy(func(msg []byte) bool {
		return bytes.HasPrefix(msg, []byte(gpbft.DOMAIN_SEPARATION_TAG+":"+pt.networkName))
	})
}

func (pt *participantTestSubject) matchTicketSigningPayload() any {
	return mock.MatchedBy(func(msg []byte) bool {
		return bytes.HasPrefix(msg, []byte(gpbft.DOMAIN_SEPARATION_TAG_VRF+":"+pt.networkName))
	})
}

func generateRandomBytes(rng *rand.Rand) []byte {
	var wantSignature [32]byte
	rng.Read(wantSignature[:])
	return wantSignature[:]
}

func TestParticipant(t *testing.T) {
	t.Parallel()
	const seed = 984651320
	signature := []byte("barreleye")

	t.Run("panic is recovered", func(t *testing.T) {
		t.Run("on Start", func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, 0)
			subject.host.On("GetProposalForInstance", subject.instance).Panic("saw me no chain")
			require.NotPanics(t, func() {
				require.ErrorContains(t, subject.Start(), "saw me no chain")
			})
		})
		t.Run("on ReceiveAlarm", func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, 0)
			subject.host.On("GetProposalForInstance", subject.instance).Panic("saw me no chain")
			require.NotPanics(t, func() {
				require.ErrorContains(t, subject.ReceiveAlarm(), "saw me no chain")
			})
		})
		t.Run("on ValidateMessage", func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, 0)
			subject.requireStart()
			require.NotPanics(t, func() {
				gotValidated, gotErr := subject.ValidateMessage(nil)
				require.Nil(t, gotValidated)
				require.Error(t, gotErr)
			})
		})
		t.Run("on ReceiveMessage", func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, 0)
			subject.requireStart()
			require.NotPanics(t, func() {
				gotErr := subject.ReceiveMessage(nil)
				require.Error(t, gotErr)
			})
		})
	})
	t.Run("when not started", func(t *testing.T) {
		t.Run("message is validated", func(t *testing.T) {
			initialInstance := uint64(0)
			subject := newParticipantTestSubject(t, seed, initialInstance)
			subject.mockCommitteeForInstance(initialInstance, subject.powerTable, subject.beacon)
			gotValidated, gotValidateErr := subject.ValidateMessage(&gpbft.GMessage{
				Sender: subject.id,
				Vote:   gpbft.Payload{},
			})
			require.Nil(t, gotValidated)
			require.ErrorContains(t, gotValidateErr, "invalid vote step: 0")
		})
		t.Run("message is accepted (queued)", func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, 0)
			gotReceiveErr := subject.ReceiveMessage(Validated(new(gpbft.GMessage)))
			require.NoError(t, gotReceiveErr)
		})
		t.Run("instance is begun", func(t *testing.T) {
			t.Run("on ReceiveAlarm", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 0)
				subject.expectBeginInstance()
				require.NoError(t, subject.ReceiveAlarm())
				subject.assertHostExpectations()
				subject.requireInstanceRoundPhase(0, 0, gpbft.QUALITY_PHASE)
			})
			t.Run("on Start", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 47)
				subject.expectBeginInstance()
				require.NoError(t, subject.Start())
				subject.assertHostExpectations()
				subject.requireInstanceRoundPhase(47, 0, gpbft.QUALITY_PHASE)
			})
			t.Run("on SkipTToInstance", func(t *testing.T) {
				// initialize participant in instance 47
				subject := newParticipantTestSubject(t, seed, 47)
				subject.host.On("Time").Return(subject.time)
				subject.host.EXPECT().SetAlarm(subject.time)
				// expect an update of the participate to 57
				fInstance := uint64(57)
				subject.instance = fInstance
				subject.expectBeginInstance()
				// Receiving the certificate should skip directly to the finality instance.
				require.NoError(t, subject.StartInstanceAt(fInstance, subject.time))
				// set subject to the finality instance to see if participant
				// has begun the right instance.
				require.NoError(t, subject.ReceiveAlarm())
				subject.assertHostExpectations()
				subject.requireInstanceRoundPhase(57, 0, gpbft.QUALITY_PHASE)
			})
		})
		t.Run("instance is not begun", func(t *testing.T) {
			t.Run("on zero canonical chain", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 0)
				var zeroChain gpbft.ECChain
				emptySupplementalData := new(gpbft.SupplementalData)
				subject.host.On("GetProposalForInstance", subject.instance).Return(emptySupplementalData, zeroChain, nil)
				require.ErrorContains(t, subject.Start(), "cannot be zero-valued")
				subject.assertHostExpectations()
				subject.requireNotStarted()
			})
			t.Run("on invalid canonical chain", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 0)
				invalidChain := gpbft.ECChain{gpbft.TipSet{}}
				emptySupplementalData := new(gpbft.SupplementalData)
				subject.host.On("GetProposalForInstance", subject.instance).Return(emptySupplementalData, invalidChain, nil)
				require.ErrorContains(t, subject.Start(), "invalid canonical chain")
				subject.assertHostExpectations()
				subject.requireNotStarted()
			})
			t.Run("on failure to fetch chain", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 0)
				invalidChain := gpbft.ECChain{gpbft.TipSet{}}
				emptySupplementalData := new(gpbft.SupplementalData)
				subject.host.On("GetProposalForInstance", subject.instance).Return(emptySupplementalData, invalidChain, errors.New("fish"))
				require.ErrorContains(t, subject.Start(), "fish")
				subject.assertHostExpectations()
				subject.requireNotStarted()
			})
			t.Run("on failure to fetch committee", func(t *testing.T) {
				subject := newParticipantTestSubject(t, seed, 0)
				chain := gpbft.ECChain{gpbft.TipSet{
					Epoch:       0,
					Key:         []byte("key"),
					PowerTable:  []byte("pt"),
					Commitments: [32]byte{},
				}}
				supplementalData := &gpbft.SupplementalData{
					PowerTable: chain[0].PowerTable,
				}
				subject.host.On("GetProposalForInstance", subject.instance).Return(supplementalData, chain, nil)
				subject.host.On("GetCommitteeForInstance", subject.instance).Return(nil, nil, errors.New("fish"))
				require.ErrorContains(t, subject.Start(), "fish")
				subject.assertHostExpectations()
				subject.requireNotStarted()
			})
		})
	})
	t.Run("when started", func(t *testing.T) {
		t.Run("on ReceiveMessage", func(t *testing.T) {
			const initialInstance = 47
			tests := []struct {
				name      string
				message   func(subject *participantTestSubject) *gpbft.GMessage
				wantErr   string
				wantTrace string
			}{
				{
					name: "prior instance message is dropped",
					message: func(subject *participantTestSubject) *gpbft.GMessage {
						return &gpbft.GMessage{
							Vote: gpbft.Payload{Instance: initialInstance - 1},
						}
					},
					wantTrace: "dropping message from old instance",
				},
				{
					name: "current instance message with unexpected base is rejected",
					message: func(subject *participantTestSubject) *gpbft.GMessage {
						require.NoError(subject.t, subject.powerTable.Add(somePowerEntry))
						return &gpbft.GMessage{
							Sender: somePowerEntry.ID,
							Vote: gpbft.Payload{
								Instance:         initialInstance,
								Step:             gpbft.QUALITY_PHASE,
								SupplementalData: *subject.supplementalData,
								Value:            gpbft.ECChain{gpbft.TipSet{Epoch: 0, Key: []byte("wrong")}},
							},
							Signature: signature,
						}
					},
					wantErr: "unexpected base",
				},
				{
					name: "current instance message with unexpected supplement is rejected",
					message: func(subject *participantTestSubject) *gpbft.GMessage {
						require.NoError(subject.t, subject.powerTable.Add(somePowerEntry))
						return &gpbft.GMessage{
							Sender: somePowerEntry.ID,
							Vote: gpbft.Payload{
								Instance: initialInstance,
								Step:     gpbft.QUALITY_PHASE,
								SupplementalData: gpbft.SupplementalData{
									Commitments: [32]byte{},
									PowerTable:  []byte("wrong"),
								},
								Value: subject.canonicalChain,
							},

							Signature: signature,
						}
					},
					wantErr: "unexpected supplement",
				},
				{
					name: "future instance message with unexpected base is queued",
					message: func(subject *participantTestSubject) *gpbft.GMessage {
						require.NoError(subject.t, subject.powerTable.Add(somePowerEntry))
						return &gpbft.GMessage{
							Sender: somePowerEntry.ID,
							Vote: gpbft.Payload{
								Instance:         initialInstance + 1,
								Step:             gpbft.QUALITY_PHASE,
								SupplementalData: *subject.supplementalData,
								Value:            gpbft.ECChain{gpbft.TipSet{Epoch: 0, Key: []byte("wrong")}},
							},
							Signature: signature,
						}
					},
				},
				{
					name: "valid current instance message is accepted",
					message: func(subject *participantTestSubject) *gpbft.GMessage {
						require.NoError(subject.t, subject.powerTable.Add(somePowerEntry))
						return &gpbft.GMessage{
							Sender: somePowerEntry.ID,
							Vote: gpbft.Payload{
								Instance:         initialInstance,
								Step:             gpbft.QUALITY_PHASE,
								SupplementalData: *subject.supplementalData,
								Value:            subject.canonicalChain,
							},
							Signature: signature,
						}
					},
				},
			}
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					subject := newParticipantTestSubject(t, seed, initialInstance)
					subject.requireStart()
					gotErr := subject.ReceiveMessage(Validated(test.message(subject)))
					if test.wantErr == "" {
						require.NoError(t, gotErr)
					} else {
						require.ErrorContains(t, gotErr, test.wantErr)
					}
					if test.wantTrace != "" {
						var found bool
						for _, msg := range subject.trace {
							require.Contains(t, msg, test.wantTrace)
							found = true
						}
						require.True(t, found, "trace %s not found", test.wantTrace)
					}
				})
			}
		})
	})
}

func TestParticipant_ValidateMessage(t *testing.T) {
	const (
		seed                  = 894651320
		initialInstanceNumber = 47
	)
	var (
		zeroPowerEntry = gpbft.PowerEntry{
			ID:     1613,
			Power:  gpbft.NewStoragePower(0),
			PubKey: gpbft.PubKey("fishmonger"),
		}
		signature = []byte("barreleye")
	)
	tests := []struct {
		name    string
		msg     func(*participantTestSubject) *gpbft.GMessage
		msgs    func(*participantTestSubject) []*gpbft.GMessage
		wantErr string
	}{
		{
			name: "valid message is accepted",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.QUALITY_PHASE,
						Value:    subject.canonicalChain,
					},
					Signature: signature,
				}
			},
		},
		{
			name: "far future instanceID is rejected",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				subject.mockCommitteeUnavailableForInstance(initialInstanceNumber + 5)
				return &gpbft.GMessage{
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber + 5,
					},
				}
			},
			wantErr: "committee not available",
		},
		{
			name: "zero message is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
					},
				}
			},
			wantErr: "sender 0 with zero power or not in power table",
		},
		{
			name: "unknown power is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: 42,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
					},
				}
			},
			wantErr: "sender 42 with zero power or not in power table",
		},
		{
			name: "zero power is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: zeroPowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
					},
				}
			},
			wantErr: "sender 1613 with zero power or not in power table",
		},
		{
			name: "invalid value chain is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.QUALITY_PHASE,
						Value:    gpbft.ECChain{*subject.canonicalChain.Base(), gpbft.TipSet{}},
					},
				}
			},
			wantErr: "invalid message vote value chain",
		},
		{
			name: "zero vote is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
					},
				}
			},
			wantErr: "invalid vote step: 0",
		},
		{
			name: "unknown vote step is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     42,
					},
				}
			},
			wantErr: "invalid vote step: 42",
		},
		{
			name: "QUALITY with non-zero vote round is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.QUALITY_PHASE,
						Round:    7,
					},
				}
			},
			wantErr: "unexpected round 7 for quality phase",
		},
		{
			name: "QUALITY with zero vote value is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.QUALITY_PHASE,
					},
				}
			},
			wantErr: "unexpected zero value for quality phase",
		},
		{
			name: "CONVERGE with zero vote round is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.CONVERGE_PHASE,
					},
				}
			},
			wantErr: "unexpected round 0 for converge phase",
		},
		{
			name: "CONVERGE with zero vote value is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.CONVERGE_PHASE,
						Round:    42,
					},
				}
			},
			wantErr: "unexpected zero value for converge phase",
		},
		{
			name: "CONVERGE with invalid vote value is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.CONVERGE_PHASE,
						Round:    42,
						Value:    gpbft.ECChain{*subject.canonicalChain.Base(), gpbft.TipSet{}},
					},
				}
			},
			wantErr: "invalid message vote value chain",
		},
		{
			name: "CONVERGE with unverified ticket is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				ticket := gpbft.Ticket("fish-cake")
				subject.mockInvalidTicket(somePowerEntry.PubKey, ticket)
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.CONVERGE_PHASE,
						Round:    42,
						Value:    subject.canonicalChain,
					},
					Ticket: ticket,
				}
			},
			wantErr: "failed to verify ticket from 1513",
		},
		{
			name: "DECIDE with non-zero vote round is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.DECIDE_PHASE,
						Round:    42,
					},
				}
			},
			wantErr: "unexpected non-zero round 42 for decide phase",
		},
		{
			name: "DECIDE with zero vote value is error",
			msg: func(*participantTestSubject) *gpbft.GMessage {
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.DECIDE_PHASE,
					},
				}
			},
			wantErr: "unexpected zero value for decide phase",
		},
		{
			name: "invalid vote signature is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				subject.mockInvalidSignature(somePowerEntry.PubKey, signature)
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.DECIDE_PHASE,
						Value:    subject.canonicalChain,
					},
					Signature: signature,
				}
			},
			wantErr: "invalid signature",
		},
		{
			name: "non nil Justification when not needed is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				nonNilJustification := &gpbft.Justification{}
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.PREPARE_PHASE,
						},
						Signature:     signature,
						Justification: nonNilJustification,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.QUALITY_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature:     signature,
						Justification: nonNilJustification,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.COMMIT_PHASE,
						},
						Signature:     signature,
						Justification: nonNilJustification,
					},
				}
			},
			wantErr: "has unexpected justification",
		},
		{
			name: "nil Justification when needed is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.DECIDE_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.COMMIT_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
					},
				}
			},
			wantErr: "has no justification",
		},
		{
			name: "CONVERGE with nil Justification when needed is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				ticket := gpbft.Ticket("fishcake")
				subject.mockValidTicket(somePowerEntry.PubKey, ticket)
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.CONVERGE_PHASE,
							Round:    4,
							Value:    subject.canonicalChain,
						},
						Ticket:    ticket,
						Signature: signature,
					},
				}
			},
			wantErr: "has no justification",
		},
		{
			name: "justification and vote instance mismatch is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.COMMIT_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Instance: initialInstanceNumber + 3,
							},
						},
					},
				}
			},
			wantErr: "has evidence from instanceID: 50",
		},
		{
			name: "justification at unexpected phase is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				subject.mockValidTicket(somePowerEntry.PubKey, signature).Maybe()
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Round:    22,
							Step:     gpbft.CONVERGE_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Step:     gpbft.CONVERGE_PHASE,
								Instance: initialInstanceNumber,
							},
						},
						Ticket: signature,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.COMMIT_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Step:     gpbft.DECIDE_PHASE,
								Instance: initialInstanceNumber,
							},
						},
						Ticket: signature,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Step:     gpbft.DECIDE_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Step:     gpbft.QUALITY_PHASE,
								Instance: initialInstanceNumber,
							},
						},
						Ticket: signature,
					},
				}
			},
			wantErr: "has justification with unexpected phase",
		},
		{
			name: "justification from wrong round is error",
			msgs: func(subject *participantTestSubject) []*gpbft.GMessage {
				subject.mockValidTicket(somePowerEntry.PubKey, signature).Maybe()
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return []*gpbft.GMessage{
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Round:    22,
							Step:     gpbft.CONVERGE_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Step:     gpbft.COMMIT_PHASE,
								Round:    22,
								Instance: initialInstanceNumber,
							},
						},
						Ticket: signature,
					},
					{
						Sender: somePowerEntry.ID,
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Round:    22,
							Step:     gpbft.CONVERGE_PHASE,
							Value:    subject.canonicalChain,
						},
						Signature: signature,
						Justification: &gpbft.Justification{
							Vote: gpbft.Payload{
								Step:     gpbft.PREPARE_PHASE,
								Round:    22,
								Instance: initialInstanceNumber,
							},
						},
						Ticket: signature,
					},
				}
			},
			wantErr: "has justification from wrong round",
		},
		{
			name: "justification with invalid value is error",
			msg: func(subject *participantTestSubject) *gpbft.GMessage {
				subject.mockValidSignature(somePowerEntry.PubKey, signature)
				return &gpbft.GMessage{
					Sender: somePowerEntry.ID,
					Vote: gpbft.Payload{
						Instance: initialInstanceNumber,
						Step:     gpbft.COMMIT_PHASE,
						Value:    subject.canonicalChain,
					},
					Signature: signature,
					Justification: &gpbft.Justification{
						Vote: gpbft.Payload{
							Instance: initialInstanceNumber,
							Value:    gpbft.ECChain{*subject.canonicalChain.Base(), gpbft.TipSet{}},
						},
					},
				}
			},
			wantErr: "invalid justification vote value chain",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			subject := newParticipantTestSubject(t, seed, initialInstanceNumber)
			require.NoError(t, subject.powerTable.Add(somePowerEntry))
			subject.requireStart()
			testValidate := func(msg *gpbft.GMessage) {
				gotValidated, gotValidateErr := subject.ValidateMessage(msg)
				subject.assertHostExpectations()
				if test.wantErr != "" {
					require.ErrorContains(t, gotValidateErr, test.wantErr)
				} else {
					require.NoError(t, gotValidateErr)
				}
				require.Equal(t, gotValidated != nil, gotValidateErr == nil)
			}

			if test.msg != nil {
				testValidate(test.msg(subject))
			}
			if test.msgs != nil {
				for _, msg := range test.msgs(subject) {
					testValidate(msg)
				}
			}
		})
	}
}

func TestParticipant_ValidateMessageParallel(t *testing.T) {
	const (
		seed                  = 894651320
		initialInstanceNumber = 47
		// Empirically, these values cause a race to be detected reliably if the mutex is removed.
		concurrency   = 10
		msgCount      = 100
		instanceCount = 5
	)
	signature := []byte("barreleye")
	subject := newParticipantTestSubject(t, seed, initialInstanceNumber)
	require.NoError(t, subject.powerTable.Add(somePowerEntry))
	subject.requireStart()
	subject.mockValidSignature(somePowerEntry.PubKey, signature)

	// Expect fetching committee for each subsequent instance (only once, since it's cached).
	for i := uint64(1); i < instanceCount; i++ {
		subject.mockCommitteeForInstance(initialInstanceNumber+i, subject.powerTable, subject.beacon)
	}

	validateOne := func(i uint64) {
		msg := &gpbft.GMessage{
			Sender: somePowerEntry.ID,
			Vote: gpbft.Payload{
				Instance: initialInstanceNumber + (i % instanceCount),
				Step:     gpbft.QUALITY_PHASE,
				Value:    subject.canonicalChain,
			},
			Signature: signature,
		}

		gotValidated, gotValidateErr := subject.ValidateMessage(msg)
		require.NoError(t, gotValidateErr)
		require.Equal(t, gotValidated != nil, gotValidateErr == nil)
	}
	// Run validation in parallel.
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < msgCount; j++ {
				validateOne(uint64(j))
			}
		}()
	}
	wg.Wait()
	subject.assertHostExpectations()
}

type validatedMessage struct {
	msg *gpbft.GMessage
}

var _ gpbft.ValidatedMessage = (*validatedMessage)(nil)

func Validated(msg *gpbft.GMessage) gpbft.ValidatedMessage {
	return &validatedMessage{msg: msg}
}

func (v *validatedMessage) Message() *gpbft.GMessage {
	return v.msg
}
