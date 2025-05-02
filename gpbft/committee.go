package gpbft

import (
	"context"
	"fmt"
	"sync"
)

var _ CommitteeProvider = (*cachedCommitteeProvider)(nil)

type cachedCommitteeProvider struct {
	delegate CommitteeProvider

	// mu guards access to committees.
	mu         sync.Mutex
	committees map[uint64]*Committee
}

func newCachedCommitteeProvider(delegate CommitteeProvider) *cachedCommitteeProvider {
	return &cachedCommitteeProvider{
		delegate:   delegate,
		committees: make(map[uint64]*Committee),
	}
}

func (c *cachedCommitteeProvider) GetCommittee(ctx context.Context, instance uint64) (*Committee, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if committee, found := c.committees[instance]; found {
		return committee, nil
	}
	switch committee, err := c.delegate.GetCommittee(ctx, instance); {
	case err != nil:
		return nil, fmt.Errorf("instance %d: %w: %w", instance, ErrValidationNoCommittee, err)
	case committee == nil:
		return nil, fmt.Errorf("unexpected nil committee for instance %d", instance)
	default:
		c.committees[instance] = committee
		return committee, nil
	}
}

// EvictCommitteesBefore evicts any cached committees that correspond to
// instances prior to the given instance.
func (c *cachedCommitteeProvider) EvictCommitteesBefore(instance uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.committees {
		if i < instance {
			delete(c.committees, i)
		}
	}
}
