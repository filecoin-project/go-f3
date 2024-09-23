package caching

import (
	"container/list"
	"sync"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("f3/internal/caching")

type GroupedSet struct {
	maxGroups int
	setPool   sync.Pool

	mu      sync.Mutex
	groups  map[uint64]*orderedSet
	recency *list.List
}

type orderedSet struct {
	order *list.Element
	*Set
}

func (os *orderedSet) Clear() {
	os.order = nil
	os.Set.Clear()
}

func NewGroupedSet(maxGroups, maxSetSize int) *GroupedSet {
	return &GroupedSet{
		maxGroups: maxGroups,
		groups:    make(map[uint64]*orderedSet, maxGroups),
		setPool: sync.Pool{
			New: func() any {
				return &orderedSet{
					Set: NewSet(maxSetSize),
				}
			},
		},
		recency: list.New(),
	}
}

// Contains checks if the given value at given group is present, and if so
// updates its recency. Otherwise, returns false.
func (gs *GroupedSet) Contains(g uint64, namespace, v []byte) (bool, error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if set, exists := gs.groups[g]; exists {
		gs.recency.MoveToFront(set.order)
		return set.Contains(namespace, v)
	}
	return false, nil
}

// Add attempts to add the given value for the given group if not already present.
func (gs *GroupedSet) Add(g uint64, namespace, v []byte) (bool, error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	set, exists := gs.groups[g]
	if !exists {
		if len(gs.groups) >= gs.maxGroups {
			if evictee := gs.recency.Back(); evictee != nil {
				gs.evict(evictee.Value.(uint64))
			}
		}
		set = gs.setPool.Get().(*orderedSet)
		set.order = gs.recency.PushFront(g)
		gs.groups[g] = set
	}
	contained, err := set.ContainsOrAdd(namespace, v)
	if err != nil {
		return false, err
	}
	return !contained, nil
}

func (gs *GroupedSet) RemoveGroupsLessThan(group uint64) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	var evictedAtLeastOne bool
	for g := range gs.groups {
		if g < group {
			evictedAtLeastOne = gs.evict(g) || evictedAtLeastOne
		}
	}
	return evictedAtLeastOne
}

func (gs *GroupedSet) evict(group uint64) bool {
	set, exists := gs.groups[group]
	if !exists {
		return false
	}
	gs.recency.Remove(set.order)
	delete(gs.groups, group)

	set.Clear()
	gs.setPool.Put(set)
	log.Debugw("Evicted grouped set from cache", "group", group)
	return true
}
