package polling

import (
	"cmp"
	"container/heap"
	"math/rand"
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	hitMissSlidingWindow = 10
	maxBackoffExponent   = 8
	// The default number of requests to make.
	defaultRequests = 8
	// The minimum number of requests to make. If we pick fewer than this number of peers, we'll
	// randomly sample known peers to find more.
	minRequests = 4
	// The maximum number of requests to make, even if all of our peers appear to be unreliable.
	maxRequests = 32
)

type peerState int

const (
	peerEvil peerState = iota - 1
	peerInactive
	peerActive
)

// TODO: Track latency and connectedness.
type peerRecord struct {
	sequentialFailures int

	// Sliding windows of hits/misses (0-10 each). If either would exceed 10, we subtract 1 from
	// both (where 0 is the floor).
	//
	// - We use sliding windows to give more weight to recent hits/misses.
	// - We don't use a simple weighted moving average because that doesn't track how "sure" we
	//   are of the measurement.
	hits, misses int

	state peerState
}

type backoffHeap []*backoffRecord

type backoffRecord struct {
	peer peer.ID
	// Delay until a round. XXX maybe do this in terms of wall-clock time?
	delayUntil int
}

type peerTracker struct {
	// TODO: garbage collect this.
	peers map[peer.ID]*peerRecord
	// TODO: Limit the number of active peers.
	active  []peer.ID
	backoff backoffHeap
	round   int
}

func (r *peerRecord) Cmp(other *peerRecord) int {
	if c := cmp.Compare(r.state, other.state); c != 0 {
		return c
	}
	rateA, countA := r.hitRate()
	rateB, countB := other.hitRate()

	if c := cmp.Compare(rateA, rateB); c != 0 {
		return c
	}
	if c := cmp.Compare(countA, countB); c != 0 {
		return c
	}
	return 0
}

// Len implements heap.Interface.
func (b *backoffHeap) Len() int {
	return len(*b)
}

// Less implements heap.Interface.
func (b *backoffHeap) Less(i int, j int) bool {
	return (*b)[i].delayUntil < (*b)[j].delayUntil
}

// Pop implements heap.Interface.
func (b *backoffHeap) Pop() any {
	return (*b)[len(*b)-1]
}

// Push implements heap.Interface.
func (b *backoffHeap) Push(x any) {
	*b = append(*b, x.(*backoffRecord))
}

// Swap implements heap.Interface.
func (b *backoffHeap) Swap(i int, j int) {
	(*b)[i], (*b)[j] = (*b)[j], (*b)[i]
}

// Records a failed request and returns how many rounds we should avoid picking this peer for.
func (r *peerRecord) recordFailure() int {
	r.sequentialFailures++
	r.state = peerInactive
	return 1 << min(r.sequentialFailures, maxBackoffExponent)
}

func (r *peerRecord) recordHit() {
	r.sequentialFailures = 0
	if r.hits < hitMissSlidingWindow {
		r.hits++
	} else if r.misses > 0 {
		r.misses--
	}
}

func (r *peerRecord) recordMiss() {
	r.sequentialFailures = 0
	if r.misses < hitMissSlidingWindow {
		r.misses++
	} else if r.hits > 0 {
		r.hits--
	}
}

// Return the hit rate and the
func (r *peerRecord) hitRate() (float64, int) {
	total := r.hits + r.misses
	// set the default rate such that we we ask `defaultRequests` peers by default.
	rate := float64(1) / defaultRequests
	if total > 0 {
		rate = float64(r.hits) / float64(total)
	}
	return rate, total

}

func (t *peerTracker) getOrCreate(p peer.ID) *peerRecord {
	r, ok := t.peers[p]
	if !ok {
		r = new(peerRecord)
		t.peers[p] = r
	}
	return r
}

func (t *peerTracker) recordInvalid(p peer.ID) {
	t.getOrCreate(p).state = peerEvil
}

func (t *peerTracker) recordMiss(p peer.ID) {
	t.getOrCreate(p).recordMiss()
}

func (t *peerTracker) recordFailure(p peer.ID) {
	// When we fail to query a peer, backoff that peer.
	r := &backoffRecord{
		peer:       p,
		delayUntil: t.round + t.getOrCreate(p).recordFailure(),
	}
	heap.Push(&t.backoff, r)
}

func (t *peerTracker) recordHit(p peer.ID) {
	t.getOrCreate(p).recordHit()
}

func (t *peerTracker) makeActive(p peer.ID) {
	r := t.getOrCreate(p)
	if r.state != peerInactive {
		return
	}
	r.state = peerActive
	t.active = append(t.active, p)
}

func (t *peerTracker) peerSeen(p peer.ID) {
	if _, ok := t.peers[p]; !ok {
		t.peers[p] = &peerRecord{state: peerActive}
		t.active = append(t.active, p)
	}
}

// Suggest a number of peers from which to request new certificates based on their historical
// record.
//
// TODO: Add a multiplier if we're not making progress.
func (t *peerTracker) suggestPeers() []peer.ID {
	// XXX: this should be a param.
	const targetProbability = 1.1

	// Advance the round and move peers from backoff to active, if necessary.
	t.round++
	for t.backoff.Len() > 0 {
		r := t.backoff[0]
		if r.delayUntil > t.round {
			break
		}
		heap.Pop(&t.backoff)
		t.makeActive(r.peer)
	}

	// Sort from best to worst.
	slices.SortFunc(t.active, func(a, b peer.ID) int {
		return t.getOrCreate(b).Cmp(t.getOrCreate(a))
	})
	// Trim off any inactive/evil peers from the end, they'll be sorted last.
	for l := len(t.active); l > 0 && t.getOrCreate(t.active[l-1]).state != peerActive; l-- {
		t.active = t.active[:l]
	}
	var prob float64
	var peerCount int
	for _, p := range t.active {
		hitRate, _ := t.getOrCreate(p).hitRate()
		// If we believe this and all the rest of the peers are useless, choose the rest of
		// the peers randomly.
		if hitRate == 0 {
			break
		}

		prob += hitRate
		peerCount++
		if peerCount >= maxRequests {
			break
		}
		// Keep going till we're 110% sure.
		if prob >= targetProbability {
			break
		}
	}

	chosen := t.active[:peerCount:peerCount]

	if peerCount == len(t.active) {
		// We've chosen all peers, nothing else we can do.
	} else if prob < targetProbability {
		// If we failed to reach the target probability, choose randomly from the remaining
		// peers.
		chosen = append(chosen, choose(t.active[peerCount:], maxRequests-peerCount)...)
	} else if peerCount < minRequests {
		// If we reached the target probability but didn't reach the number of minimum
		// requests, pick a few more peers to fill us out.
		chosen = append(chosen, choose(t.active[peerCount:], minRequests-peerCount)...)
	}

	return chosen
}

var _ heap.Interface = new(backoffHeap)

func choose[T any](items []T, count int) []T {
	if len(items) <= count {
		return items
	}

	// Knuth 3.4.2S. Could use rand.Perm, but that would allocate a large array.
	// There are more efficient algorithms for small sample sizes, but they're complex.
	chosen := make([]T, 0, count)
	for t := 0; len(chosen) < cap(chosen); t++ {
		if rand.Intn(len(items)-t) < cap(chosen)-len(chosen) {
			chosen = append(chosen, items[t])
		}
	}
	return chosen
}
