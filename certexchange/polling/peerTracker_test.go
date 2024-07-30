package polling

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/require"
)

func TestPeerRecordHitMiss(t *testing.T) {
	r := new(peerRecord)

	{
		hitRate, count := r.hitRate()
		// should initialize to some "assumed" positive value.
		require.Greater(t, hitRate, 0.0)
		require.Equal(t, 0, count)
	}

	// 3 hits
	for i := 1; i <= 3; i++ {
		r.recordHit()
		hitRate, count := r.hitRate()
		require.Equal(t, 1.0, hitRate)
		require.Equal(t, min(i, hitMissSlidingWindow), count)
	}

	// 3 misses
	for i := 4; i <= 6; i++ {
		r.recordMiss()
		hitRate, count := r.hitRate()
		require.Less(t, hitRate, 1.0)
		require.Equal(t, min(i, hitMissSlidingWindow), count)
	}

	// Should be 50/50.
	{
		hitRate, count := r.hitRate()
		require.Equal(t, min(6, hitMissSlidingWindow), count)
		require.Equal(t, 0.5, hitRate)
	}

	// 2 hits
	for i := 0; i < 2; i++ {
		r.recordHit()
	}

	{
		hitRate, count := r.hitRate()
		require.Less(t, hitRate, 1.0)
		require.Equal(t, hitMissSlidingWindow, count)
	}

	// 3 more hits
	for i := 0; i <= 3; i++ {
		r.recordHit()
	}

	// Should now be 100% hit (canceling out the misses).
	{
		hitRate, count := r.hitRate()
		require.Equal(t, hitRate, 1.0)
		require.Equal(t, hitMissSlidingWindow, count)
	}

	// should bring us back to 50/50

	for i := 0; i < hitMissSlidingWindow; i++ {
		r.recordMiss()
	}

	{
		hitRate, _ := r.hitRate()
		require.Equal(t, hitRate, 0.5)
	}

	// Another 10 should bring us to 0.0

	for i := 0; i < 10; i++ {
		r.recordMiss()
	}

	{
		hitRate, _ := r.hitRate()
		require.Equal(t, hitRate, 0.0)
	}
}

func TestPeerRecordExponentialBackoff(t *testing.T) {
	r := new(peerRecord)
	require.Equal(t, 1, r.recordFailure())
	require.Equal(t, 2, r.recordFailure())
	require.Equal(t, 4, r.recordFailure())

	// clears backoff.
	r.recordHit()
	require.Equal(t, 1, r.recordFailure())

	// backoff stops eventually
	for i := 0; i < 100; i++ {
		r.recordFailure()
	}
	require.Equal(t, 1<<maxBackoffExponent, r.recordFailure())
}

func TestPeerTracker(t *testing.T) {
	pt := newPeerTracker(clock.NewMock())
	ctx := context.Background()

	var peers []peer.ID
	discoverPeers := func(count int) {
		for i := 0; i < count; i++ {
			p := test.RandPeerIDFatal(t)
			peers = append(peers, p)
			pt.peerSeen(p)
		}
	}

	for _, n := range []int{0, 1, defaultRequests / 2, defaultRequests/2 - 1} {
		discoverPeers(n)
		pt.lastHitRound = pt.currentRound
		suggested := pt.suggestPeers(ctx)
		require.ElementsMatch(t, peers, suggested)
	}

	// Too many peers
	discoverPeers(1)
	pt.lastHitRound = pt.currentRound
	require.Less(t, len(pt.suggestPeers(ctx)), len(peers))

	// fail a peer and we should pick the other peers now.
	pt.recordMiss(peers[0])
	pt.lastHitRound = pt.currentRound

	require.ElementsMatch(t, peers[1:], pt.suggestPeers(ctx))

	// Now ensure we select that peer. It should be first because it's the best.
	pt.recordHit(peers[0])
	pt.recordHit(peers[0])

	pt.recordHit(peers[1])
	pt.recordMiss(peers[1]) // needs to be worse than peer 1

	require.Equal(t, pt.suggestPeers(ctx)[0], peers[0])

	// Now check to make sure we backoff that peer.
	{
		pt.recordFailure(peers[0])
		pt.recordMiss(peers[1])
		suggested := pt.suggestPeers(ctx)
		require.Equal(t, 1, pt.backoff.Len())
		require.NotContains(t, suggested, peers[0])
		require.Equal(t, suggested[0], peers[1])

		// Peer 0 should be back on-top after one round absent.
		suggested = pt.suggestPeers(ctx)
		require.Empty(t, pt.backoff)
		require.Equal(t, suggested[0], peers[0])
	}

	// Should last two rounds the second time (exponential).

	{
		pt.recordFailure(peers[0])
		pt.recordFailure(peers[1])

		suggested := pt.suggestPeers(ctx)
		require.Equal(t, 2, pt.backoff.Len())
		require.NotContains(t, suggested, peers[0])
		require.NotContains(t, suggested, peers[1])

		suggested = pt.suggestPeers(ctx)
		require.Equal(t, 1, pt.backoff.Len())
		require.Contains(t, suggested, peers[1])
		require.NotContains(t, suggested, peers[0])

		suggested = pt.suggestPeers(ctx)
		require.Len(t, pt.backoff, 0)
		require.Contains(t, pt.active, peers[0])
		require.Contains(t, suggested, peers[0])
		require.Contains(t, suggested, peers[1])
	}

	// Then four rounds.
	pt.recordFailure(peers[0])
	require.NotContains(t, pt.suggestPeers(ctx), peers[0])
	require.NotContains(t, pt.suggestPeers(ctx), peers[0])
	require.NotContains(t, pt.suggestPeers(ctx), peers[0])
	require.NotContains(t, pt.suggestPeers(ctx), peers[0])
	require.Contains(t, pt.suggestPeers(ctx), peers[0])

	// Now, give that peer a perfect success rate
	for i := 0; i < 100; i++ {
		pt.recordHit(peers[0])
	}

	// We always pick at least 4 peers, even if we have high confidence in one.
	{
		suggested := pt.suggestPeers(ctx)
		require.Len(t, suggested, minRequests)
		require.Equal(t, peers[0], suggested[0])
	}

	// Now mark that peer as evil, we should never pick it again.
	pt.recordInvalid(peers[0])
	for i := 0; i < 5; i++ {
		require.NotContains(t, pt.suggestPeers(ctx), peers[0])

		// No matter what we do.
		pt.recordHit(peers[0])
		pt.peerSeen(peers[0])
	}

	pt.recordHit(peers[1])
	require.Contains(t, pt.suggestPeers(ctx), peers[1])
	pt.lastHitRound = pt.currentRound

	// Discover a whole bunch of peers.
	discoverPeers(gcHighWater)

	// We should have garbage collected
	require.Less(t, len(pt.peers), gcHighWater)

	{
		// The new peers shouldn't affect out discovered peers.
		require.Contains(t, pt.suggestPeers(ctx), peers[1])
	}

	// We should never suggest more than 32 peers at a time.
	{
		for p := range pt.peers {
			for i := 0; i < 10; i++ {
				pt.recordMiss(p)
			}
		}

		require.Len(t, pt.suggestPeers(ctx), maxRequests)
	}

}
