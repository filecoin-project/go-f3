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
	MakeTicket(beacon []byte, instance uint64, round uint64, signer ActorID) Ticket
}

type VRFTicketVerifier interface {
<<<<<<< HEAD
	VerifyTicket(beacon []byte, instance uint64, round uint64, signer PubKey, ticket Ticket) bool
=======
	VerifyTicket(beacon []byte, instance uint32, round uint32, signer PubKey, ticket Ticket) bool
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
}

// VRF used for the CONVERGE step of GossiPBFT.
type VRF struct {
	signer   Signer
	verifier Verifier
}

func NewVRF(signer Signer, verifier Verifier) *VRF {
	return &VRF{
		signer:   signer,
		verifier: verifier,
	}
}

func (f *VRF) MakeTicket(beacon []byte, instance uint64, round uint64, signer ActorID) Ticket {
	return f.signer.Sign(signer, f.serializeSigInput(beacon, instance, round))
}

<<<<<<< HEAD
func (f *VRF) VerifyTicket(beacon []byte, instance uint64, round uint64, signer PubKey, ticket Ticket) bool {
=======
func (f *VRF) VerifyTicket(beacon []byte, instance uint32, round uint32, signer PubKey, ticket Ticket) bool {
>>>>>>> 6ee56ae (Ensure signing and verifying modifies no input)
	return f.verifier.Verify(signer, f.serializeSigInput(beacon, instance, round), ticket)
}

// Serializes the input to the VRF signature for the CONVERGE step of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func (f *VRF) serializeSigInput(beacon []byte, instance uint64, round uint64) []byte {
	// TODO: DST
	buf := make([]byte, 0, len(beacon)+8+8)
	buf = append(buf, beacon...)
	buf = binary.BigEndian.AppendUint64(buf, instance)
	buf = binary.BigEndian.AppendUint64(buf, round)

	return buf
}
