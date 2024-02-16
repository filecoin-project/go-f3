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
	MakeTicket(beacon []byte, instance uint64, round uint64, networkName NetworkName) (Ticket, error)
}

type VRFTicketVerifier interface {
	VerifyTicket(beacon []byte, instance uint64, round uint64, signer PubKey, networkName NetworkName, ticket Ticket) bool
}

// VRF used for the CONVERGE step of GossiPBFT.
type VRF struct {
	source   PubKey
	signer   Signer
	verifier Verifier
}

func NewVRF(source PubKey, signer Signer, verifier Verifier) *VRF {
	return &VRF{
		source:   source,
		signer:   signer,
		verifier: verifier,
	}
}

func (f *VRF) MakeTicket(beacon []byte, instance uint64, round uint64, networkName NetworkName) (Ticket, error) {
	return f.signer.Sign(f.source, f.serializeSigInput(beacon, instance, round, networkName))
}

func (f *VRF) VerifyTicket(beacon []byte, instance uint64, round uint64, signer PubKey, networkName NetworkName, ticket Ticket) bool {
	return f.verifier.Verify(signer, f.serializeSigInput(beacon, instance, round, networkName), ticket) == nil
}

const DOMAIN_SEPARATION_TAG_VRF = "VRF"

// Serializes the input to the VRF signature for the CONVERGE step of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func (f *VRF) serializeSigInput(beacon []byte, instance uint64, round uint64, networkName NetworkName) []byte {
	var buf bytes.Buffer

	buf.WriteString(DOMAIN_SEPARATION_TAG_VRF)
	buf.WriteString(":")
	buf.WriteString(string(networkName))
	buf.WriteString(":")
	buf.Write(beacon)
	buf.WriteString(":")
	_ = binary.Write(&buf, binary.BigEndian, instance)
	_ = binary.Write(&buf, binary.BigEndian, round)

	return buf.Bytes()
}
