package sim

import (
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
	"golang.org/x/xerrors"
)

// Receives and validates finality decisions
type DecisionLog struct {
	Network  gpbft.Network
	Verifier gpbft.Verifier
	// Base tipset for each instance.
	Bases []gpbft.TipSet
	// Powertable for each instance.
	PowerTables []*gpbft.PowerTable
	// Decisions received for each instance and participant.
	Decisions []map[gpbft.ActorID]*gpbft.Justification
	err       error
}

func NewDecisionLog(ntwk gpbft.Network, verifier gpbft.Verifier) *DecisionLog {
	return &DecisionLog{
		Network:  ntwk,
		Verifier: verifier,
	}
}

// Establishes the base tipset and power table for a new instance.
func (dl *DecisionLog) BeginInstance(instance uint64, base gpbft.TipSet, power *gpbft.PowerTable) {
	if instance != uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d is not the next instance", instance))
	}
	dl.Bases = append(dl.Bases, base)
	dl.PowerTables = append(dl.PowerTables, power)
	dl.Decisions = append(dl.Decisions, make(map[gpbft.ActorID]*gpbft.Justification))
}

// Checks whether all participants (except any adversary) have decided on a value for an instance.
func (dl *DecisionLog) HasCompletedInstance(instance uint64, adversary gpbft.ActorID) bool {
	if instance >= uint64(len(dl.Bases)) {
		return false
	}
	target := len(dl.PowerTables[instance].Entries)
	if dl.PowerTables[instance].Has(adversary) {
		target -= 1
	}

	return len(dl.Decisions[instance]) == target
}

// Checks that all participants (except any adversary) for an instance decided on the same value.
func (dl *DecisionLog) VerifyInstance(instance uint64, adversary gpbft.ActorID) error {
	if dl.err != nil {
		return dl.err
	}
	if instance >= uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d not yet begun", instance))
	}
	// Check each actor in the power table decided, and their decisions matched.
	var first gpbft.ECChain
	for _, powerEntry := range dl.PowerTables[instance].Entries {
		decision, ok := dl.Decisions[instance][powerEntry.ID]
		if powerEntry.ID == adversary {
			continue
		}
		if !ok {
			return fmt.Errorf("actor %d did not decide", powerEntry.ID)
		}
		if first.IsZero() {
			first = decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(first) {
			return fmt.Errorf("actor %d decided %v, but first actor decided %v",
				powerEntry.ID, decision.Vote.Value, first)
		}
	}
	return nil
}

func (dl *DecisionLog) PrintInstance(instance uint64) {
	if instance >= uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d not yet begun", instance))
	}
	var first gpbft.ECChain
	for _, powerEntry := range dl.PowerTables[instance].Entries {
		decision, ok := dl.Decisions[instance][powerEntry.ID]
		if !ok {
			fmt.Printf("‼️ Participant %d did not decide\n", powerEntry.ID)
		}
		if first.IsZero() {
			first = decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(first) {
			fmt.Printf("‼️ Participant %d decided %v, but %d decided %v\n",
				powerEntry.ID, decision.Vote, dl.PowerTables[instance].Entries[0].ID, first)
		}
	}
}

// Verifies and records a decision from a participant.
// Returns whether this was the first decision for the instance.
func (dl *DecisionLog) ReceiveDecision(participant gpbft.ActorID, decision *gpbft.Justification) (first bool) {
	if err := dl.verifyDecision(decision); err == nil {
		if len(dl.Decisions[decision.Vote.Instance]) == 0 {
			first = true
		}
		// Record the participant's decision.
		dl.Decisions[decision.Vote.Instance][participant] = decision
	} else {
		fmt.Printf("invalid decision: %v\n", err)
		dl.err = err
	}
	return
}

func (dl *DecisionLog) verifyDecision(decision *gpbft.Justification) error {
	if decision.Vote.Instance >= uint64(len(dl.Bases)) {
		return fmt.Errorf("decision for future instance: %v", decision)
	}
	base := dl.Bases[decision.Vote.Instance]
	powerTable := dl.PowerTables[decision.Vote.Instance]
	if decision.Vote.Step != gpbft.DECIDE_PHASE {
		return fmt.Errorf("decision for wrong phase: %v", decision)
	}
	if decision.Vote.Round != 0 {
		return fmt.Errorf("decision for wrong round: %v", decision)
	}
	if decision.Vote.Value.IsZero() {
		return fmt.Errorf("decided empty tipset: %v", decision)
	}
	if !decision.Vote.Value.HasBase(base) {
		return fmt.Errorf("decided tipset with wrong base: %v", decision)
	}

	// Extract signers.
	justificationPower := gpbft.NewStoragePower(0)
	signers := make([]gpbft.PubKey, 0)
	if err := decision.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(powerTable.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		justificationPower.Add(justificationPower, powerTable.Entries[bit].Power)
		signers = append(signers, powerTable.Entries[bit].PubKey)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}
	// Check signers have strong quorum
	strongQuorum := gpbft.NewStoragePower(0)
	strongQuorum = strongQuorum.Mul(strongQuorum, gpbft.NewStoragePower(2))
	strongQuorum = strongQuorum.Div(strongQuorum, gpbft.NewStoragePower(3))
	if justificationPower.Cmp(strongQuorum) < 0 {
		return fmt.Errorf("decision lacks strong quorum: %v", decision)
	}
	// Verify aggregate signature
	payload := decision.Vote.MarshalForSigning(dl.Network.NetworkName())
	if err := dl.Verifier.VerifyAggregate(payload, decision.Signature, signers); err != nil {
		return xerrors.Errorf("invalid aggregate signature: %v: %w", decision, err)
	}
	return nil
}
