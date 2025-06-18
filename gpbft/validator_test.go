package gpbft_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/emulator"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/caching"
	"github.com/stretchr/testify/require"
)

func TestValidator(t *testing.T) {
	var (
		ctx               = context.Background()
		plausibleProposal = &gpbft.ECChain{
			TipSets: []*gpbft.TipSet{tipset0, tipSet1, tipSet2},
		}
		// validVoteSignature is the placeholder to signal that valid signature should be
		// populated before test.
		validVoteSignature = []byte("valid_vote_signature")
	)
	type testCase struct {
		name                string
		scenario            validatorTestScenario
		givenMessage        *gpbft.GMessage
		givenPartialMessage *gpbft.PartialGMessage
		wantError           error
	}

	for _, test := range []testCase{
		{
			name:      "nil message",
			wantError: gpbft.ErrValidationInvalid,
		},
		{
			name: "valid partial signature",
			scenario: validatorTestScenario{
				InstantProgress: gpbft.InstanceProgress{
					Instant: gpbft.Instant{
						Phase: gpbft.QUALITY_PHASE,
					},
					Input: plausibleProposal,
				},
				CommitteeLookback: 10,
				Committees: map[uint64]map[gpbft.ActorID]int{
					0: {
						1: 10,
					},
				},
				CacheMaxGroups:  10,
				CacheMaxSetSize: 10,
			},
			givenMessage: &gpbft.GMessage{
				Sender: 1,
				Vote: gpbft.Payload{
					Phase: gpbft.QUALITY_PHASE,
					Value: plausibleProposal,
				},
				Signature: validVoteSignature,
			},
			givenPartialMessage: &gpbft.PartialGMessage{
				GMessage: &gpbft.GMessage{
					Sender: 1,
					Vote: gpbft.Payload{
						Phase: gpbft.QUALITY_PHASE,
					},
					Signature: validVoteSignature,
				},
				VoteValueKey: plausibleProposal.Key(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			environment := newValidatorTestEnvironment(test.scenario)
			subject := environment.newTestSubject()
			if test.givenMessage != nil {
				msg := test.givenMessage
				if bytes.Equal(msg.Signature, validVoteSignature) {
					signingPayload := msg.Vote.MarshalForSigning(test.scenario.NetworkName)
					committee, err := environment.GetCommittee(ctx, msg.Vote.Instance)
					require.NoError(t, err)
					_, key := committee.PowerTable.Get(msg.Sender)
					msg.Signature, err = environment.signing.Sign(ctx, key, signingPayload)
					require.NoError(t, err)
				}
			}
			if test.givenPartialMessage != nil {
				pmsg := test.givenPartialMessage
				if pmsg.GMessage != nil && bytes.Equal(pmsg.GMessage.Signature, validVoteSignature) {
					signingPayload := pmsg.GMessage.Vote.MarshalForSigningWithValueKey(test.scenario.NetworkName, pmsg.VoteValueKey)
					committee, err := environment.GetCommittee(ctx, pmsg.Vote.Instance)
					require.NoError(t, err)
					_, key := committee.PowerTable.Get(pmsg.Sender)
					pmsg.Signature, err = environment.signing.Sign(ctx, key, signingPayload)
					require.NoError(t, err)
				}

			}
			valid, err := subject.ValidateMessage(ctx, test.givenMessage)
			if test.wantError != nil {
				require.ErrorIs(t, err, test.wantError, "expected error %q, got %v", test.wantError, err)
				require.Nil(t, valid)
			} else {
				require.NoError(t, err, "expected no error, got %v", err)
				require.NotNil(t, valid, "expected message to be valid, but it was not")
			}

			partiallyValid, err := subject.PartiallyValidateMessage(ctx, test.givenPartialMessage)
			if test.wantError != nil {
				require.ErrorIs(t, err, test.wantError, "expected error %q, got %v", test.wantError, err)
				require.Nil(t, partiallyValid)
			} else {
				require.NoError(t, err, "expected no error, got %v", err)
				require.NotNil(t, partiallyValid, "expected partial message to be valid, but it was not")
			}
		})
	}
}

type validatorTestScenario struct {
	NetworkName       gpbft.NetworkName
	InstantProgress   gpbft.InstanceProgress
	CommitteeLookback uint64
	Committees        map[uint64]map[gpbft.ActorID]int // Maps instance ID to participant to storage power
	CacheMaxGroups    int
	CacheMaxSetSize   int
}

var (
	_ gpbft.Verifier          = (*validatorTestEnvironment)(nil)
	_ gpbft.CommitteeProvider = (*validatorTestEnvironment)(nil)
)

type validatorTestEnvironment struct {
	scenario validatorTestScenario
	progress gpbft.Progress
	cache    *caching.GroupedSet
	signing  emulator.Signing
}

func newValidatorTestEnvironment(scenario validatorTestScenario) *validatorTestEnvironment {
	return &validatorTestEnvironment{
		scenario: scenario,
		progress: func() gpbft.InstanceProgress { return scenario.InstantProgress },
		cache:    caching.NewGroupedSet(scenario.CacheMaxGroups, scenario.CacheMaxSetSize),
		signing:  emulator.AdhocSigning(),
	}
}

func (v *validatorTestEnvironment) GetCommittee(ctx context.Context, instance uint64) (*gpbft.Committee, error) {
	if v.scenario.Committees == nil {
		return nil, errors.New("no committees for any instance")
	}
	committee, ok := v.scenario.Committees[instance]
	if !ok {
		return nil, fmt.Errorf("no committee for instance %d", instance)
	}
	powerEntries := make([]gpbft.PowerEntry, 0, len(committee))
	publicKeys := make([]gpbft.PubKey, 0, len(committee))
	for actor, power := range committee {
		pk := []byte(fmt.Sprintf("actor: %d", actor))
		powerEntries = append(powerEntries, gpbft.PowerEntry{
			ID:     actor,
			Power:  gpbft.NewStoragePower(int64(power)),
			PubKey: pk,
		})
		publicKeys = append(publicKeys, pk)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	pt := gpbft.NewPowerTable()
	if err := pt.Add(powerEntries...); err != nil {
		return nil, fmt.Errorf("failed to create power table: %w", err)
	}
	aggregate, err := v.signing.Aggregate(publicKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate public keys: %w", err)
	}
	return &gpbft.Committee{
		PowerTable:        pt,
		Beacon:            []byte(fmt.Sprintf("🥓: %d", instance)),
		AggregateVerifier: aggregate,
	}, ctx.Err()
}

func (v *validatorTestEnvironment) Verify(pubKey gpbft.PubKey, msg, sig []byte) error {
	return v.signing.Verify(pubKey, msg, sig)
}

func (v *validatorTestEnvironment) Aggregate(pubKeys []gpbft.PubKey) (gpbft.Aggregate, error) {
	return v.signing.Aggregate(pubKeys)
}

func (v *validatorTestEnvironment) newTestSubject() gpbft.Validator {
	return gpbft.NewValidator(v.scenario.NetworkName, v, v, v.progress, v.cache, v.scenario.CommitteeLookback)
}
