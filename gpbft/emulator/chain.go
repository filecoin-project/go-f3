package emulator

import (
	"fmt"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ gpbft.Chain = (*chain)(nil)

type chain map[uint64]*Instance

func newChain() chain { return make(chain) }

func (e chain) getInstance(id uint64) *Instance { return e[id] }

func (e chain) setDecision(decision *gpbft.Justification) error {
	switch instance := e.getInstance(decision.Vote.Instance); {
	case instance == nil:
		return fmt.Errorf("cannot set decision for unknown instance ID: %d", decision.Vote.Instance)
	case instance.decision != nil:
		return fmt.Errorf("instance %d has already been decided", decision.Vote.Instance)
	default:
		instance.decision = decision
		return nil
	}
}

func (e chain) GetProposalForInstance(id uint64) (*gpbft.SupplementalData, gpbft.ECChain, error) {
	instance := e.getInstance(id)
	if instance == nil {
		return nil, nil, fmt.Errorf("instance ID %d not found", id)
	}
	return &instance.supplementalData, instance.GetProposal(), nil
}

func (e chain) GetCommitteeForInstance(id uint64) (power *gpbft.PowerTable, beacon []byte, err error) {
	instance := e.getInstance(id)
	if instance == nil {
		return nil, nil, fmt.Errorf("instance ID %d not found", id)
	}
	return instance.powerTable, instance.beacon, nil
}

func (e chain) setInstance(instance *Instance) error {
	if existing := e.getInstance(instance.id); existing != nil {
		return fmt.Errorf("instance ID %d is already set", instance.id)
	}
	e[instance.id] = instance
	return nil
}
