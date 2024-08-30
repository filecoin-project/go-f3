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

// Contains checks if the given sample v is contained within sample set. A
// namespace may optionally be specified for the value. Namespace must be between
// 0 to 64 bytes long.
func (ss *Set) Contains(namespace, v []byte) (bool, error) {
	key, err := ss.newKey(namespace, v)
	if err != nil {
		return false, err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, exists := ss.flip[key]; exists {
		return true, nil
	}
	if _, exists := ss.flop[key]; exists {
		return true, nil
	}
	return false, nil
}

// ContainsOrAdd checks if the given sample v is contained within sample set, and
// if not adds it. The namespace may be optionally supplied; it must be between
// zero to 64 bytes.
func (ss *Set) ContainsOrAdd(namespace, v []byte) (bool, error) {
	key, err := ss.newKey(namespace, v)
	if err != nil {
		return false, err
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Check if the sample exists in either sets and if not insert it.
	if _, exists := ss.flip[key]; exists {
		return true, nil
	}
	if _, exists := ss.flop[key]; exists {
		return true, nil
	}
	ss.flip[key] = struct{}{}

	// Check if flip exceeds maxSize and if so do the flippity flop.
	if len(ss.flip) >= ss.maxSize {
		clear(ss.flop)
		ss.flop, ss.flip = ss.flip, ss.flop
		log.Debugw("Cleared flop and swapped subsets as max size is reached", "maxSize", ss.maxSize)
	}
	return false, nil
}

func (ss *Set) newKey(namespace, v []byte) ([32]byte, error) {
	hasher, err := blake2b.New(blake2b.Size256, namespace)
	if err != nil {
		return [32]byte{}, err
	}
	_, err = hasher.Write(v)
	if err != nil {
		return [32]byte{}, err
	}
	var key [32]byte
	keyB := hasher.Sum(key[:0])
	copy(key[:], keyB)
	// TODO: using blake's built-in keys means we lose the nice [32]byte return types
	//       from Sum256. Consider pooling byte slices.
	return key, nil
}

// Clear removes all elements in the set.
func (ss *Set) Clear() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.flip = make(map[[32]byte]struct{})
	ss.flop = make(map[[32]byte]struct{})
}
