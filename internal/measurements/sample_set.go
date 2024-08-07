package measurements

import "sync"

// SampleSet stores a bounded set of samples and exposes the ability to check
// whether it contains a given sample. See SampleSet.contains.
//
// Internally, SampleSet uses two maps to store samples, each of which can grow
// up to the specified max size. When one map fills up, the SampleSet switches to
// the other, effectively flipping between them. This allows the set to check for
// sample existence across a range of approximately max size to twice the max size,
// offering a larger sample set compared to implementations that track insertion
// order with similar memory footprint.
//
// The worst case memory footprint of SampleSet is around 2 * maxSize * 96
// bytes.
type SampleSet struct {

	// We can use existing LRU implementations for this at the price of slightly
	// higher memory footprint and explanation that recency is unused. Hence the
	// hand-rolled data structure here.

	// maxSize defines the maximum number of samples to store per each internal set.
	maxSize int
	// mu protects access to flip and flop.
	mu sync.Mutex
	// flip stores one set of samples until until it reaches maxSize.
	flip map[string]struct{}
	// flop stores another set of samples until it reaches maxSize.
	flop map[string]struct{}
}

// NewSampleSet creates a new SampleSet with a specified max size per sample
// subset.
func NewSampleSet(maxSize int) *SampleSet {
	maxSize = max(1, maxSize)
	return &SampleSet{
		maxSize: maxSize,
		flip:    make(map[string]struct{}, maxSize),
		flop:    make(map[string]struct{}, maxSize),
	}
}

// Contains checks if the given sample v is contained within sample set, and if
// not adds it.
func (ss *SampleSet) Contains(v []byte) bool {
	// The number 96 comes from the length of BLS signatures, the kind of value we
	// expect as the argument. Defensively re-slice it if it is larger at the price
	// of losing accuracy.
	//
	// Alternatively we could hash the values but considering the memory footprint of
	// these measurements (sub 10MB for a total of 50K samples) we choose larger
	// memory consumption over CPU footprint.
	key := string(v[:min(len(v), 96)])

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
	}
	return false
}
