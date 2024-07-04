package polling

import (
	"cmp"
	"container/heap"
	"math/rand"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	hitMissSlidingWindow = 3
	maxBackoffExponent   = 8
	// The default number of requests to make.
	defaultRequests = 8
	// The minimum number of requests to make. If we pick fewer than this number of peers, we'll
	// randomly sample known peers to find more.
	minRequests = 4
	// The maximum number of requests to make, even if all of our peers appear to be unreliable.
	maxRequests = 32
	// How confident should we be that we've suggested enough peers. 1.125 == 112.5%
	targetConfidence = 1.125

	gcHighWater      = 10000 // GC the oldest peers when we have 10k
	gcLowWater       = 1000  // GC down to 1000 peers
	gcFailureCuttoff = 3     // Ignore failure counts below N when garbage collecting.

	// EWMA alpha for latency tracking (0-1, smaller numbers favor newer readings)
	latencyAlpha = 0.7
)

type peerState int

const (
	peerEvil peerState = iota - 1
	peerInactive
	peerDeactivating
	peerActive
)

// TODO: Track latency and connectedness.
type peerRecord struct {
	id peer.ID
	// Number of sequential failures since the last successful request.
	sequentialFailures int
	// Sliding windows of hits/misses (0-3 each). If either would exceed 3, we subtract 1 from
	// both (where 0 is the floor).
	//
	// - We use sliding windows to give more weight to recent hits/misses.
	// - We don't use a simple weighted moving average because that doesn't track how "sure" we
	//   are of the measurement.
	hits, misses int
	state        peerState
	lastSeen     time.Time
	latency      time.Duration
}

type backoffHeap []*backoffRecord

type backoffRecord struct {
	peer peer.ID
	// Delay until a round. XXX maybe do this in terms of wall-clock time?
	delayUntil int
}

func newPeerTracker() *peerTracker {
	return &peerTracker{
		peers: make(map[peer.ID]*peerRecord),
	}
}

type peerTracker struct {
	peers                      map[peer.ID]*peerRecord
	active                     []peer.ID
	backoff                    backoffHeap
	lastHitRound, currentRound int
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

	// If we have latency measurements for both, prefer the peer with lower latency.
	if r.latency > 0 && other.latency > 0 {
		if c := cmp.Compare(other.latency, r.latency); c != 0 {
			return c
		}
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
	s := (*b)
	item := s[len(s)-1]
	s[len(s)-1] = nil
	*b = s[:len(s)-1]
	return item
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
	delay := 1 << min(r.sequentialFailures, maxBackoffExponent)

	// failures are misses as well.
	if r.misses < hitMissSlidingWindow {
		r.misses++
	} else if r.hits > 0 {
		r.hits--
	}

	r.sequentialFailures++
	if r.state == peerActive {
		r.state = peerDeactivating
	}

	return delay
}

func (r *peerRecord) updateLatency(d time.Duration) {
	if r.latency > 0 {
		r.latency += time.Duration(latencyAlpha * float64(d-r.latency))
	} else {
		r.latency = d
	}
}

func (r *peerRecord) recordHit() {
	r.sequentialFailures = 0
	r.lastSeen = time.Now()
	if r.hits < hitMissSlidingWindow {
		r.hits++
	} else if r.misses > 0 {
		r.misses--
	}
}

func (r *peerRecord) recordMiss() {
	r.sequentialFailures = 0
	r.lastSeen = time.Now()
	if r.misses < hitMissSlidingWindow {
		r.misses++
	} else if r.hits > 0 {
		r.hits--
	}
}

func (r *peerRecord) recordInvalid() {
	r.state = peerEvil
	r.sequentialFailures = 0
	r.lastSeen = time.Now()
}

// Return the hit rate a number between 0-10 indicating how "full" our window is.
func (r *peerRecord) hitRate() (float64, int) {
	total := r.hits + r.misses
	// set the default rate such that we we ask `defaultRequests` peers by default.
	rate := targetConfidence / defaultRequests
	if total > 0 {
		rate = float64(r.hits) / float64(total)
	}
	return rate, min(total, hitMissSlidingWindow)

}

func (t *peerTracker) getOrCreate(p peer.ID) *peerRecord {
	r, ok := t.peers[p]
	if !ok {
		now := clk.Now()
		r = &peerRecord{id: p, lastSeen: now}
		t.peers[p] = r
	}
	return r
}

func (t *peerTracker) updateLatency(p peer.ID, d time.Duration) {
	t.getOrCreate(p).updateLatency(d)
}

func (t *peerTracker) recordInvalid(p peer.ID) {
	t.getOrCreate(p).recordInvalid()
}

func (t *peerTracker) recordMiss(p peer.ID) {
	t.getOrCreate(p).recordMiss()
}

func (t *peerTracker) recordFailure(p peer.ID) {
	// When we fail to query a peer, backoff that peer.
	r := &backoffRecord{
		peer:       p,
		delayUntil: t.currentRound + t.getOrCreate(p).recordFailure(),
	}
	heap.Push(&t.backoff, r)
}

func (t *peerTracker) recordHit(p peer.ID) {
	t.lastHitRound = t.currentRound
	t.getOrCreate(p).recordHit()
}

// Reactivate a peer from backoff.
func (t *peerTracker) reactivate(p peer.ID) {
	r, ok := t.peers[p]
	if !ok {
		// If we're not still tracking this peer, just forget about them.
		return
	}
	switch r.state {
	case peerEvil, peerActive:
		return
	case peerInactive:
		t.active = append(t.active, p)
	case peerDeactivating:
	}
	r.state = peerActive
}

func (t *peerTracker) peerSeen(p peer.ID) {
	now := clk.Now()
	if r, ok := t.peers[p]; !ok {
		t.peers[p] = &peerRecord{id: p, state: peerActive, lastSeen: now}
		t.active = append(t.active, p)
		t.maybeGc()
	} else {
		r.lastSeen = now
	}
}

// Garbage collect peers down to our "low" water mark (1000)
func (t *peerTracker) maybeGc() {
	if len(t.peers) < gcHighWater {
		return
	}
	eligible := make([]*peerRecord, 0, len(t.peers))

	for _, r := range t.peers {
		eligible = append(eligible, r)
	}

	slices.SortFunc(eligible, func(a, b *peerRecord) int {
		// Evil peers always sort later.
		if (a.state == peerEvil) != (b.state == peerEvil) {
			return cmp.Compare(b.state, a.state)
		}

		// Peers with 3+ sequential failures are considered "worse" than all peers with
		// fewer failures.
		if (a.sequentialFailures < gcFailureCuttoff) != (b.sequentialFailures < gcFailureCuttoff) {
			return cmp.Compare(a.sequentialFailures, b.sequentialFailures)
		}

		// Then compare success metrics.

		hitRateA, hitCountA := a.hitRate()
		hitRateB, hitCountB := b.hitRate()

		// If exactly one of the peers has a 0 hit-rate, it's always worse than the other.
		if (hitRateA == 0) != (hitRateB == 0) {
			return cmp.Compare(hitRateB, hitRateA)
		}

		// If we have no information on one peer but have information on the other, prefer
		// the peer we have information on.
		if (hitCountA == 0) != (hitCountB == 0) {
			return cmp.Compare(hitCountB, hitCountA)
		}

		// Otherwise, pick the peer with the higher hit rate.
		if c := cmp.Compare(hitRateB, hitRateA); c != 0 {
			return c
		}

		// Otherwise, pick the peer with more information.
		if c := cmp.Compare(hitCountB, hitCountA); c != 0 {
			return c
		}

		// Finally, exclude the peer with the most request failures.
		if c := cmp.Compare(a.sequentialFailures, b.sequentialFailures); c != 0 {
			return c
		}

		return 0
	})

	// gc
	for _, r := range eligible[gcLowWater:] {
		delete(t.peers, r.id)
	}

	// filter active peers
	t.active = slices.DeleteFunc(t.active, func(p peer.ID) bool {
		_, ok := t.peers[p]
		return !ok
	})

	// filter backoff
	t.backoff = slices.DeleteFunc(t.backoff, func(r *backoffRecord) bool {
		_, ok := t.peers[r.peer]
		return !ok
	})
	heap.Init(&t.backoff)
}

// Advance the round and move peers from backoff to active, if necessary.
func (t *peerTracker) advanceRound() {
	for t.backoff.Len() > 0 {
		r := t.backoff[0]
		if r.delayUntil > t.currentRound {
			break
		}
		heap.Pop(&t.backoff)
		t.reactivate(r.peer)
	}
	t.currentRound++
}

// Re-rank peers.
func (t *peerTracker) rank() {
	// Sort from best to worst.
	slices.SortFunc(t.active, func(a, b peer.ID) int {
		return t.getOrCreate(b).Cmp(t.getOrCreate(a))
	})

	// Trim off any inactive/evil peers from the end, they'll be sorted last.
	activePeers := len(t.active)
trimLoop:
	for ; activePeers > 0; activePeers-- {
		r := t.getOrCreate(t.active[activePeers-1])
		switch r.state {
		case peerActive:
			break trimLoop
		case peerDeactivating:
			r.state = peerInactive
		}
	}
	clear(t.active[activePeers:])
	t.active = t.active[:activePeers]
}

// Suggest a number of peers from which to request new certificates based on their historical
// record.
func (t *peerTracker) suggestPeers() []peer.ID {
	t.advanceRound()
	t.maybeGc()
	t.rank()

	// Adjust the minimum peer count and probability threshold based on the current distance to
	// the last successful round (capped at 8 rounds).
	// - We increase the minimum peer threshold exponentially till we hit the max.
	// - We increase the target confidence lineally. We still want to try more and more of our
	//   "good" peers but... as we keep failing, we want to try more and more random peers.
	distance := min(8, t.currentRound-t.lastHitRound)
	minPeers := min(minRequests<<(distance-1), maxRequests)
	threshold := targetConfidence * float64(distance)

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
		if prob >= threshold {
			break
		}
	}

	chosen := t.active[:peerCount:peerCount]

	if peerCount == len(t.active) {
		// We've chosen all peers, nothing else we can do.
	} else if prob < threshold {
		// If we failed to reach the target probability, choose randomly from the remaining
		// peers.
		chosen = append(chosen, choose(t.active[peerCount:], maxRequests-peerCount)...)
	} else if peerCount < minPeers {
		// If we reached the target probability but didn't reach the number of minimum
		// requests, pick a few more peers to fill us out.
		chosen = append(chosen, choose(t.active[peerCount:], minPeers-peerCount)...)
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
