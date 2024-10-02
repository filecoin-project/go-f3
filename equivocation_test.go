package f3

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var localGoodPID = peer.ID("1local")
var remotePID0 = peer.ID("0remote")
var remotePID5 = peer.ID("5remote")

func TestEquivocationFilter_ProcessBroadcast(t *testing.T) {
	localPID := localGoodPID
	ef := newEquivocationFilter(localPID)

	// Test case 1: First message should be processed
	msg1 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature1"),
	}
	require.True(t, ef.ProcessBroadcast(msg1), "First message should be processed")

	// Test case 2: Duplicate message with same signature should be processed
	msg2 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature1"),
	}
	require.True(t, ef.ProcessBroadcast(msg2), "Duplicate message with same signature should be processed")

	// Test case 3 Message with same key but different signature should not be processed
	msg3 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature2"),
	}
	require.False(t, ef.ProcessBroadcast(msg3), "Message with same key but different signature should not be processed")

	// Test case 4: Message with new instance should be processed
	msg4 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 2, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature3"),
	}
	require.True(t, ef.ProcessBroadcast(msg4), "Message with new instance should be processed")

	// Test case 5: Message with past instance should not be processed
	msg5 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature4"),
	}
	require.False(t, ef.ProcessBroadcast(msg5), "Message with past instance should not be processed")
}

func TestEquivocationFilter_formKey(t *testing.T) {
	ef := newEquivocationFilter(localGoodPID)

	msg := &gpbft.GMessage{
		Sender: gpbft.ActorID(1),
		Vote:   gpbft.Payload{Round: 1, Phase: gpbft.Phase(1)},
	}

	expectedKey := equivocationKey{
		Sender: gpbft.ActorID(1),
		Round:  1,
		Phase:  gpbft.Phase(1),
	}

	require.Equal(t, expectedKey, ef.formKey(msg), "Keys should match")
}

func TestEquivocationFilter_remoteEquivocationResolution(t *testing.T) {
	localPID := localGoodPID
	ef := newEquivocationFilter(localPID)

	msg1 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature1"),
	}
	require.True(t, ef.ProcessBroadcast(msg1), "First message should be processed")
	ef.ProcessReceive(localPID, msg1)

	msg2 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature2"),
	}
	ef.ProcessReceive(remotePID5, msg2)
	require.Contains(t, ef.activeSenders[msg2.Sender].origins, remotePID5)

	msg3 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 2, Phase: gpbft.Phase(1)},
		Signature: []byte("signature3"),
	}

	require.True(t, ef.ProcessBroadcast(msg3), "local sender should still be able to broadcast")
	ef.ProcessReceive(localPID, msg1)

	// lower PeerID sender comes along
	msg4 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 2, Phase: gpbft.Phase(1)},
		Signature: []byte("signature4"),
	}
	ef.ProcessReceive(remotePID0, msg4)
	require.Contains(t, ef.activeSenders[msg2.Sender].origins, remotePID0)

	msg5 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 3, Phase: gpbft.Phase(1)},
		Signature: []byte("signature5"),
	}

	require.False(t, ef.ProcessBroadcast(msg5), "we should have backed off")

	require.False(t, ef.ProcessBroadcast(msg3), "trying to re-broadcast is now not allowed")
}

func TestEquivocationFilter_PeerIDBasedEquivocationHandling(t *testing.T) {
	localPID := localGoodPID
	ef := newEquivocationFilter(localPID)

	// Local broadcast
	msg1 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature1"),
	}
	require.True(t, ef.ProcessBroadcast(msg1), "Local message should be processed")

	// Remote equivocation with higher PeerID
	msg2 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature2"),
	}
	ef.ProcessReceive(remotePID5, msg2)
	require.Contains(t, ef.activeSenders[msg2.Sender].origins, remotePID5, "Higher PeerID should be recorded")

	// Local broadcast after higher PeerID equivocation
	require.True(t, ef.ProcessBroadcast(msg1), "Local message should still be processed after higher PeerID equivocation")

	// Remote equivocation with lower PeerID
	msg3 := &gpbft.GMessage{
		Sender:    gpbft.ActorID(1),
		Vote:      gpbft.Payload{Instance: 1, Round: 1, Phase: gpbft.Phase(1)},
		Signature: []byte("signature3"),
	}
	ef.ProcessReceive(remotePID0, msg3)
	require.Contains(t, ef.activeSenders[msg3.Sender].origins, remotePID0, "Lower PeerID should be recorded")

	// Local broadcast after lower PeerID equivocation
	require.False(t, ef.ProcessBroadcast(msg1), "Local message should not be processed after lower PeerID equivocation")
}
