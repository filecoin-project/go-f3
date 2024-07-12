package gpbft

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoPower is returned by the MessageBuilder if the specified participant has no power.
var ErrNoPower = errors.New("no power")

type MessageBuilder struct {
	NetworkName      NetworkName
	PowerTable       powerTableAccessor
	Payload          Payload
	BeaconForTicket  []byte
	Justification    *Justification
	SigningMarshaler SigningMarshaler
}

type powerTableAccessor interface {
	Get(ActorID) (uint16, PubKey)
}

type SignerWithMarshaler interface {
	Signer
	SigningMarshaler
}

// Build uses the builder and a signer interface to build GMessage
// It is a shortcut for when separated flow is not required
func (mt *MessageBuilder) Build(ctx context.Context, signer Signer, id ActorID) (*GMessage, error) {
	st, err := mt.PrepareSigningInputs(id)
	if err != nil {
		return nil, fmt.Errorf("preparing signing inputs: %w", err)
	}

	payloadSig, vrf, err := st.Sign(ctx, signer)
	if err != nil {
		return nil, fmt.Errorf("signing message builder: %w", err)
	}

	return st.Build(payloadSig, vrf), nil
}

// SignatureBuilder's fields are exposed to facilitate JSON encoding
type SignatureBuilder struct {
	NetworkName NetworkName

	ParticipantID ActorID
	Payload       Payload
	Justification *Justification
	PubKey        PubKey
	PayloadToSign []byte
	VRFToSign     []byte
}

func (mb *MessageBuilder) PrepareSigningInputs(id ActorID) (*SignatureBuilder, error) {
	effectivePower, pubKey := mb.PowerTable.Get(id)
	if pubKey == nil || effectivePower == 0 {
		return nil, fmt.Errorf("could not find pubkey for actor %d: %w", id, ErrNoPower)
	}
	sb := SignatureBuilder{
		ParticipantID: id,
		NetworkName:   mb.NetworkName,
		Payload:       mb.Payload,
		Justification: mb.Justification,

		PubKey: pubKey,
	}

	sb.PayloadToSign = mb.SigningMarshaler.MarshalPayloadForSigning(mb.NetworkName, &mb.Payload)
	if mb.BeaconForTicket != nil {
		sb.VRFToSign = vrfSerializeSigInput(mb.BeaconForTicket, mb.Payload.Instance, mb.Payload.Round, mb.NetworkName)
	}
	return &sb, nil
}

// Sign creates the signed payload from the signature builder and returns the payload
// and VRF signatures. These signatures can be used independent from the builder.
func (st *SignatureBuilder) Sign(ctx context.Context, signer Signer) ([]byte, []byte, error) {
	payloadSignature, err := signer.Sign(ctx, st.PubKey, st.PayloadToSign)
	if err != nil {
		return nil, nil, fmt.Errorf("signing payload: %w", err)
	}
	var vrf []byte
	if st.VRFToSign != nil {
		vrf, err = signer.Sign(ctx, st.PubKey, st.VRFToSign)
		if err != nil {
			return nil, nil, fmt.Errorf("signing vrf: %w", err)
		}
	}
	return payloadSignature, vrf, nil
}

// Build takes the template and signatures and builds GMessage out of them
func (st *SignatureBuilder) Build(payloadSignature []byte, vrf []byte) *GMessage {
	return &GMessage{
		Sender:        st.ParticipantID,
		Vote:          st.Payload,
		Signature:     payloadSignature,
		Ticket:        vrf,
		Justification: st.Justification,
	}
}
