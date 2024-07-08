package gpbft

import (
	"context"
	"errors"
	"sync"

	xerrors "golang.org/x/xerrors"
)

// ErrNoPower is returned by the MessageBuilder if the specified participant has no power.
var ErrNoPower = errors.New("no power")

type MessageBuilder struct {
	lk              sync.RWMutex
	networkName     NetworkName
	powerTable      powerTableAccessor
	payload         Payload
	beaconForTicket []byte
	justification   *Justification

	signingMarshaller SigningMarshaler
}

// NewMessageBuilder creates a new message builder with the provided beacon for ticket,
// justification and payload.
func NewMessageBuilder(powerTable powerTableAccessor) *MessageBuilder {
	return &MessageBuilder{
		powerTable: powerTable,
	}
}

// SetBeaconForTicket sets the beacon for the ticket in the message builder.
func (mb *MessageBuilder) SetBeaconForTicket(b []byte) {
	mb.lk.Lock()
	defer mb.lk.Unlock()
	mb.beaconForTicket = b
}

// SetJustification sets the justification in the message builder.
func (mb *MessageBuilder) SetJustification(j *Justification) {
	mb.lk.Lock()
	defer mb.lk.Unlock()
	mb.justification = j
}

// SetPayload sets the payload in the message builder.
func (mb *MessageBuilder) SetPayload(p Payload) {
	mb.lk.Lock()
	defer mb.lk.Unlock()
	mb.payload = p
}

func (mb *MessageBuilder) Payload() Payload {
	mb.lk.RLock()
	defer mb.lk.RUnlock()
	return mb.payload
}

func (mb *MessageBuilder) BeaconForTicket() []byte {
	mb.lk.RLock()
	defer mb.lk.RUnlock()
	return mb.beaconForTicket
}

func (mb *MessageBuilder) Justification() *Justification {
	mb.lk.RLock()
	defer mb.lk.RUnlock()
	return mb.justification
}

func (mb *MessageBuilder) SetNetworkName(nn NetworkName) {
	mb.lk.Lock()
	defer mb.lk.Unlock()
	mb.networkName = nn
}

func (mb *MessageBuilder) NetworkName() NetworkName {
	mb.lk.RLock()
	defer mb.lk.RUnlock()
	return mb.networkName
}

func (mb *MessageBuilder) SetSigningMarshaler(sm SigningMarshaler) {
	mb.lk.Lock()
	defer mb.lk.Unlock()
	mb.signingMarshaller = sm
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
		return nil, xerrors.Errorf("preparing signing inputs: %w", err)
	}

	payloadSig, vrf, err := st.Sign(ctx, signer)
	if err != nil {
		return nil, xerrors.Errorf("signing message builder: %w", err)
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
	mb.lk.RLock()
	defer mb.lk.RUnlock()
	effectivePower, pubKey := mb.powerTable.Get(id)
	if pubKey == nil || effectivePower == 0 {
		return nil, xerrors.Errorf("could not find pubkey for actor %d: %w", id, ErrNoPower)
	}
	sb := SignatureBuilder{
		ParticipantID: id,
		NetworkName:   mb.networkName,
		Payload:       mb.payload,
		Justification: mb.justification,

		PubKey: pubKey,
	}

	sb.PayloadToSign = mb.signingMarshaller.MarshalPayloadForSigning(mb.networkName, &mb.payload)
	if mb.beaconForTicket != nil {
		sb.VRFToSign = vrfSerializeSigInput(mb.beaconForTicket, mb.payload.Instance, mb.payload.Round, mb.networkName)
	}
	return &sb, nil
}

// NewMessageBuilderWithPowerTable allows to create a new message builder
// from an existing power table.
//
// This is needed to sign forged messages in adversary hosts
func NewMessageBuilderWithPowerTable(power *PowerTable) *MessageBuilder {
	return &MessageBuilder{
		powerTable: power,
	}
}

// Sign creates the signed payload from the signature builder and returns the payload
// and VRF signatures. These signatures can be used independent from the builder.
func (st *SignatureBuilder) Sign(ctx context.Context, signer Signer) ([]byte, []byte, error) {
	payloadSignature, err := signer.Sign(ctx, st.PubKey, st.PayloadToSign)
	if err != nil {
		return nil, nil, xerrors.Errorf("signing payload: %w", err)
	}
	var vrf []byte
	if st.VRFToSign != nil {
		vrf, err = signer.Sign(ctx, st.PubKey, st.VRFToSign)
		if err != nil {
			return nil, nil, xerrors.Errorf("signing vrf: %w", err)
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
