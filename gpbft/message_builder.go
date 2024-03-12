package gpbft

import (
	"math/big"

	xerrors "golang.org/x/xerrors"
)

type MessageBuilder struct {
	powerTable      powerTableAccessor
	payload         Payload
	beaconForTicket []byte
	justification   *Justification
}

// NewMessageBuilder creates a new message builder with the provided beacon for ticket,
// justification and payload.
func NewMessageBuilder(powerTable powerTableAccessor) MessageBuilder {
	return MessageBuilder{
		powerTable: powerTable,
	}
}

// SetBeaconForTicket sets the beacon for the ticket in the message builder.
func (mb *MessageBuilder) SetBeaconForTicket(b []byte) {
	mb.beaconForTicket = b
}

// SetJustification sets the justification in the message builder.
func (mb *MessageBuilder) SetJustification(j *Justification) {
	mb.justification = j
}

// SetPayload sets the payload in the message builder.
func (mb *MessageBuilder) SetPayload(p Payload) {
	mb.payload = p
}

func (mb MessageBuilder) Payload() Payload {
	return mb.payload
}

func (mb MessageBuilder) BeaconForTicket() []byte {
	return mb.beaconForTicket
}

func (mb MessageBuilder) Justification() *Justification {
	return mb.justification
}

type powerTableAccessor interface {
	Get(ActorID) (uint16, *big.Int, PubKey)
}

type SignerWithMarshaler interface {
	Signer
	SigningMarshaler
}

// Build uses the builder and a signer interface to build GMessage
// It is a shortcut for when separated flow is not required
func (mt MessageBuilder) Build(networkName NetworkName, signer SignerWithMarshaler, id ActorID) (*GMessage, error) {
	st, err := mt.PrepareSigningInputs(signer, networkName, id)
	if err != nil {
		return nil, xerrors.Errorf("preparing signing inputs: %w", err)
	}

	payloadSig, vrf, err := st.sign(signer)
	if err != nil {
		return nil, xerrors.Errorf("signing message builder: %w", err)
	}

	return st.build(payloadSig, vrf), nil
}

type SignatureBuilder struct {
	NetworkName NetworkName

	participantID ActorID
	payload       Payload
	justification *Justification
	pubKey        PubKey
	payloadToSign []byte
	vrfToSign     []byte
}

func (sb *SignatureBuilder) ParticipantID() ActorID {
	return sb.participantID
}

func (sb *SignatureBuilder) Payload() Payload {
	return sb.payload
}

func (sb *SignatureBuilder) Justification() *Justification {
	return sb.justification
}

func (sb *SignatureBuilder) PubKey() PubKey {
	return sb.pubKey
}

func (sb *SignatureBuilder) PayloadToSign() []byte {
	return sb.payloadToSign
}

func (sb *SignatureBuilder) VRFToSign() []byte {
	return sb.vrfToSign
}

func (mt MessageBuilder) PrepareSigningInputs(msh SigningMarshaler, networkName NetworkName, id ActorID) (SignatureBuilder, error) {
	_, _, pubKey := mt.powerTable.Get(id)
	if pubKey == nil {
		return SignatureBuilder{}, xerrors.Errorf("could not find pubkey for actor %d", id)
	}
	sb := SignatureBuilder{
		participantID: id,
		NetworkName:   networkName,
		payload:       mt.payload,
		justification: mt.justification,

		pubKey: pubKey,
	}

	sb.payloadToSign = msh.MarshalPayloadForSigning(networkName, &mt.payload)
	if mt.beaconForTicket != nil {
		sb.vrfToSign = vrfSerializeSigInput(mt.beaconForTicket, mt.payload.Instance, mt.payload.Round, networkName)
	}
	return sb, nil
}

// NewMessageBuilderWithPowerTable allows to create a new message builder
// from an existing power table.
//
// This is needed to sign forged messages in adversary hosts
func NewMessageBuilderWithPowerTable(power *PowerTable) MessageBuilder {
	return MessageBuilder{
		powerTable: power,
	}
}

// Sign creates the signed payload from the signature builder and returns the payload
// and VRF signatures. These signatures can be used independent from the builder.
func (st SignatureBuilder) sign(signer Signer) ([]byte, []byte, error) {
	payloadSignature, err := signer.Sign(st.pubKey, st.payloadToSign)
	if err != nil {
		return nil, nil, xerrors.Errorf("signing payload: %w", err)
	}
	var vrf []byte
	if st.vrfToSign != nil {
		vrf, err = signer.Sign(st.pubKey, st.vrfToSign)
		if err != nil {
			return nil, nil, xerrors.Errorf("signing vrf: %w", err)
		}
	}
	return payloadSignature, vrf, nil
}

// Build takes the template and signatures and builds GMessage out of them
func (st SignatureBuilder) build(payloadSignature []byte, vrf []byte) *GMessage {
	return &GMessage{
		Sender:        st.participantID,
		Vote:          st.payload,
		Signature:     payloadSignature,
		Ticket:        vrf,
		Justification: st.justification,
	}
}
