package sim

import (
	"errors"
	"fmt"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/filecoin-project/go-state-types/big"
)

// simEC captures the complete simulated EC state for all instances performed in
// the lifetime of a simulation. It includes:
// - EC instances performed by the simulation.
// - base chain, power table and beacon at each instance.
// - decisions made by each participant at each instance.
//
// Additionally, it captures errors that may occur as a result if invalid
// decisions made by participants.
type simEC struct {
	instances   []*ECInstance
	networkName gpbft.NetworkName
	verifier    signing.Backend
	errors      errGroup
}

type ECInstance struct {
	Instance uint64
	// The base of all chains, which participants must agree on.
	BaseChain gpbft.ECChain
	// The power table to be used for this instance.
	PowerTable *gpbft.PowerTable
	// The beacon value to use for this instance.
	Beacon []byte
	// SupplementalData is the additional data for this instance.
	SupplementalData *gpbft.SupplementalData

	ec        *simEC
	decisions map[gpbft.ActorID]*gpbft.Justification
}

type errGroup []error

func (e errGroup) Error() string {
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("total of %d error(s)", len(e)))
	for _, err := range e {
		msg.WriteString(fmt.Sprintln())
		msg.WriteString(err.Error())
	}
	return msg.String()
}

func newEC(opts *options) *simEC {
	return &simEC{
		networkName: opts.networkName,
		verifier:    opts.signingBacked,
	}
}

func (ec *simEC) BeginInstance(baseChain gpbft.ECChain, pt *gpbft.PowerTable) *ECInstance {
	// Take beacon value from the head of the base chain.
	// Note a real beacon value will come from a finalised chain with some lookback.
	beacon := baseChain.Head().Key
	nextInstanceID := uint64(ec.Len())
	instance := &ECInstance{
		Instance:   nextInstanceID,
		BaseChain:  baseChain,
		PowerTable: pt,
		Beacon:     beacon,
		SupplementalData: &gpbft.SupplementalData{
			PowerTable: gpbft.MakeCid([]byte(fmt.Sprintf("supp-data-pt@%d", nextInstanceID))),
		},
		ec:        ec,
		decisions: make(map[gpbft.ActorID]*gpbft.Justification),
	}
	ec.instances = append(ec.instances, instance)
	return instance
}

func (ec *simEC) GetInstance(instance uint64) *ECInstance {
	if !ec.HasInstance(instance) {
		return nil
	}
	return ec.instances[instance]
}

func (ec *simEC) Len() int {
	return len(ec.instances)
}

func (ec *simEC) Err() error {
	if len(ec.errors) > 0 {
		return ec.errors
	}
	return nil
}

func (ec *simEC) NotifyDecision(participant gpbft.ActorID, decision *gpbft.Justification) {
	switch i := ec.GetInstance(decision.Vote.Instance); {
	case i != nil:
		i.NotifyDecision(participant, decision)
	default:
		err := fmt.Errorf("participant %d decided at non-existing instance %d", participant, decision.Vote.Instance)
		ec.errors = append(ec.errors, err)
	}
}

func (eci *ECInstance) validateDecision(decision *gpbft.Justification) error {
	switch {
	case eci.Instance != decision.Vote.Instance:
		return fmt.Errorf("instance mismatch: expected %d but got %d", eci.Instance, decision.Vote.Instance)
	case decision.Vote.Phase != gpbft.DECIDE_PHASE:
		return fmt.Errorf("decision for wrong phase: %s", decision.Vote.Phase)
	case decision.Vote.Round != 0:
		return fmt.Errorf("decision for wrong round: %d", decision.Vote.Round)
	case decision.Vote.Value.IsZero():
		return errors.New("decided empty tipset")
	case !decision.Vote.Value.HasBase(eci.BaseChain.Head()): // Assert that the base of decision is the head of previously agreed upon chain in the instance, i.e. instance.BaseChain.
		return fmt.Errorf("decided tipset with wrong base: %v", decision.Vote.Value.Base())
	}

	// Extract signers.
	justificationPower := gpbft.NewStoragePower(0)
	signers := make([]gpbft.PubKey, 0)
	powerTable := eci.PowerTable
	if err := decision.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(powerTable.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		justificationPower = big.Add(justificationPower, powerTable.Entries[bit].Power)
		signers = append(signers, powerTable.Entries[bit].PubKey)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}
	// Check signers have strong quorum
	strongQuorum := gpbft.NewStoragePower(0)
	strongQuorum = big.Mul(strongQuorum, gpbft.NewStoragePower(2))
	strongQuorum = big.Div(strongQuorum, gpbft.NewStoragePower(3))
	if justificationPower.LessThan(strongQuorum) {
		return fmt.Errorf("decision lacks strong quorum: %v", decision)
	}
	// Verify aggregate signature
	payload := eci.ec.verifier.MarshalPayloadForSigning(eci.ec.networkName, &decision.Vote)
	if err := eci.ec.verifier.VerifyAggregate(payload, decision.Signature, signers); err != nil {
		return fmt.Errorf("invalid aggregate signature: %v: %w", decision, err)
	}

	return nil
}

// HasReachedConsensus checks that all participants (except any adversary) for an
// instance decided on the same value.
func (eci *ECInstance) HasReachedConsensus(exclude ...gpbft.ActorID) (*gpbft.ECChain, bool) {

	exclusions := make(map[gpbft.ActorID]struct{})
	for _, id := range exclude {
		exclusions[id] = struct{}{}
	}

	// Check each actor in the power table decided, and their decisions matched.
	var consensus *gpbft.ECChain
	for _, powerEntry := range eci.PowerTable.Entries {
		if _, found := exclusions[powerEntry.ID]; found {
			continue
		}
		decision, ok := eci.decisions[powerEntry.ID]
		if !ok {
			return nil, false
		}
		if consensus == nil {
			consensus = &decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(*consensus) {
			return nil, false
		}
	}
	return consensus, true
}

// HasCompleted checks whether all participants, except any excluded ones, have
// reached some decision.
func (eci *ECInstance) HasCompleted(exclude ...gpbft.ActorID) bool {
	exclusions := make(map[gpbft.ActorID]struct{})
	for _, id := range exclude {
		exclusions[id] = struct{}{}
	}
	wantDecisions := len(eci.PowerTable.Entries)
	var gotDecisions int
	for _, entry := range eci.PowerTable.Entries {
		if _, excluded := exclusions[entry.ID]; excluded {
			wantDecisions--
		} else if _, decided := eci.decisions[entry.ID]; decided {
			gotDecisions++
		}
	}
	return wantDecisions == gotDecisions
}

func (eci *ECInstance) GetDecision(participant gpbft.ActorID) *gpbft.ECChain {
	justification, ok := eci.decisions[participant]
	if !ok {
		return nil
	}
	return &justification.Vote.Value
}

func (ec *simEC) HasInstance(instance uint64) bool {
	return ec.Len() > int(instance)
}

func (eci *ECInstance) Print() {
	var first *gpbft.ECChain
	for _, powerEntry := range eci.PowerTable.Entries {
		switch decision, ok := eci.decisions[powerEntry.ID]; {
		case !ok:
			fmt.Printf("‼️ Participant %d did not decide\n", powerEntry.ID)
		case first == nil:
			first = &decision.Vote.Value
		case !decision.Vote.Value.Eq(*first):
			fmt.Printf("‼️ Participant %d decided %v, but %d decided %v\n",
				powerEntry.ID, decision.Vote, eci.PowerTable.Entries[0].ID, first)
		}
	}
}

func (eci *ECInstance) NotifyDecision(participant gpbft.ActorID, decision *gpbft.Justification) {
	if err := eci.validateDecision(decision); err != nil {
		err := fmt.Errorf("invalid decision by participant %d: %w", participant, err)
		eci.ec.errors = append(eci.ec.errors, err)
	}
	eci.decisions[participant] = decision
}
