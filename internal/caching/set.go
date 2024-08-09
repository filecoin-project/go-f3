package caching

import (
	"sync"

	"golang.org/x/crypto/blake2b"
)

type Set struct {
	// maxSize defines the maximum number of samples to store per each internal set.
	maxSize int
	// mu protects access to flip and flop.
	mu sync.Mutex
	// flip stores one set of samples until until it reaches maxSize.
	flip map[[32]byte]struct{}
	// flop stores another set of samples until it reaches maxSize.
	flop map[[32]byte]struct{}
}

// NewSet creates a new Set with a specified max size per sample subset. The max
// size cannot be less than 1; if it is the cache will silently be instantiated
// with max size of 1.
func NewSet(maxSize int) *Set {
	maxSize = max(1, maxSize)
	return &Set{
		maxSize: maxSize,
		flip:    make(map[[32]byte]struct{}, maxSize),
		flop:    make(map[[32]byte]struct{}, maxSize),
	}
}

// Contains checks if the given sample v is contained within sample set.
func (ss *Set) Contains(v []byte) bool {
	key := blake2b.Sum256(v)
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, exists := ss.flip[key]; exists {
		return true
	}
	if _, exists := ss.flop[key]; exists {
		return true
	}
	return false
}

// ContainsOrAdd checks if the given sample v is contained within sample set, and
// if not adds it.
func (ss *Set) ContainsOrAdd(v []byte) bool {
	key := blake2b.Sum256(v)
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Check if the sample exists in either sets and if not insert it.
	if _, exists := ss.flip[key]; exists {
		return true
	}
	if _, exists := ss.flop[key]; exists {
		return true
	}
	ss.flip[key] = struct{}{}

	// Check if flip exceeds maxSize and if so do the flippity flop.
	if len(ss.flip) >= ss.maxSize {
		clear(ss.flop)
		ss.flop, ss.flip = ss.flip, ss.flop
		log.Debugw("Cleared flop and swapped subsets as max size is reached", "maxSize", ss.maxSize)
	}
	return false
}

// Clear removes all elements in the set.
func (ss *Set) Clear() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	clear(ss.flip)
	clear(ss.flop)
}
