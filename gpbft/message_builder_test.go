package gpbft

import (
	"testing"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageBuilder(t *testing.T) {
	pt := NewPowerTable()
	err := pt.Add([]PowerEntry{
		{
			ID:     0,
			PubKey: PubKey{0},
			Power:  big.NewInt(1),
		},
		{
			ID:     1,
			PubKey: PubKey{1},
			Power:  big.NewInt(1),
		},
	}...)
	assert.NoError(t, err)
	payload := Payload{
		Instance: 1,
		Round:    0,
	}
	nn := NetworkName("test")

	mt := &MessageBuilder{
		NetworkName: nn,
		PowerTable:  pt,
		Payload:     payload,
	}

	_, err = mt.PrepareSigningInputs(2)
	require.Error(t, err, "unknown ID should return an error")

	st, err := mt.PrepareSigningInputs(0)
	require.NoError(t, err)

	require.Equal(t, st.Payload, payload)
	require.Equal(t, st.ParticipantID, ActorID(0))
	require.Equal(t, st.PubKey, PubKey{0})
	require.NotNil(t, st.PayloadToSign)
	require.Nil(t, st.VRFToSign)

	st, err = mt.PrepareSigningInputs(1)
	require.NoError(t, err)

	require.Equal(t, st.Payload, payload)
	require.Equal(t, st.ParticipantID, ActorID(1))
	require.Equal(t, st.PubKey, PubKey{1})
	require.NotNil(t, st.PayloadToSign)
	require.Nil(t, st.VRFToSign)
}

func TestMessageBuilderWithVRF(t *testing.T) {
	pt := NewPowerTable()
	err := pt.Add([]PowerEntry{
		{
			ID:     0,
			PubKey: PubKey{0},
			Power:  big.NewInt(1),
		},
		{
			ID:     1,
			PubKey: PubKey{1},
			Power:  big.NewInt(1),
		},
	}...)
	assert.NoError(t, err)
	payload := Payload{
		Instance: 1,
		Round:    0,
	}

	nn := NetworkName("test")
	mt := &MessageBuilder{
		NetworkName:     nn,
		PowerTable:      pt,
		Payload:         payload,
		BeaconForTicket: []byte{0xbe, 0xac, 0x04},
	}

	st, err := mt.PrepareSigningInputs(0)
	require.NoError(t, err)

	require.Equal(t, st.Payload, payload)
	require.Equal(t, st.ParticipantID, ActorID(0))
	require.Equal(t, st.PubKey, PubKey{0})
	require.NotNil(t, st.PayloadToSign)
	require.NotNil(t, st.VRFToSign)

	st, err = mt.PrepareSigningInputs(1)
	require.NoError(t, err)

	require.Equal(t, st.Payload, payload)
	require.Equal(t, st.ParticipantID, ActorID(1))
	require.Equal(t, st.PubKey, PubKey{1})
	require.NotNil(t, st.PayloadToSign)
	require.NotNil(t, st.VRFToSign)
}
