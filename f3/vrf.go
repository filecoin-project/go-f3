package f3

import (
	"bytes"
	"encoding/binary"
)

// A ticket is a signature over some common payload.
type Ticket []byte

func (t Ticket) Compare(other Ticket) int {
	return bytes.Compare(t, other)
}

// Computes VRF tickets for use in CONVERGE phase.
// A VRF ticket is produced by signing a payload which digests a beacon randomness value and
// the instance and round numbers.
type VRFTicketSource interface {
	MakeTicket(beacon []byte, instance uint32, round uint32, signer ActorID) Ticket
}

type VRFTicketVerifier interface {
	VerifyTicket(beacon []byte, instance uint32, round uint32, signer ActorID, ticket Ticket) bool
}

// VRF used for the CONVERGE step of GossiPBFT.
type VRF struct {
	signer Signer
}

func NewVRF(signer Signer) *VRF {
	return &VRF{
		signer: signer,
	}
}

func (f *VRF) MakeTicket(beacon []byte, instance uint32, round uint32, signer ActorID) Ticket {
	return f.signer.Sign(signer, f.serializeSigInput(beacon, instance, round))
}

func (f *VRF) VerifyTicket(beacon []byte, instance uint32, round uint32, signer ActorID, ticket Ticket) bool {
	return f.signer.Verify(signer, f.serializeSigInput(beacon, instance, round), ticket)
}

// Serializes the input to the VRF signature for the CONVERGE step of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func (f *VRF) serializeSigInput(beacon []byte, instance uint32, round uint32) []byte {
	instanceBytes := make([]byte, 4)
	roundBytes := make([]byte, 4)

	binary.BigEndian.PutUint32(instanceBytes, instance)
	binary.BigEndian.PutUint32(roundBytes, round)

	sigInput := append(beacon, instanceBytes...)
	sigInput = append(sigInput, roundBytes...)

	return sigInput
}
