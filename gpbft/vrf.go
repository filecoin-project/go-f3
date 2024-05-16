package gpbft

import (
	"bytes"
	"encoding/binary"
)

// A ticket is a signature over some common payload.
type Ticket []byte

type VRFHost interface {
	Network
	Signer
	Verifier
}

func MakeTicket(beacon []byte, instance uint64, round uint64, source PubKey, host VRFHost) (Ticket, error) {
	return host.Sign(source, serializeSigInput(beacon, instance, round, host.NetworkName()))
}

func VerifyTicket(beacon []byte, instance uint64, round uint64, source PubKey, host VRFHost, ticket Ticket) bool {
	return host.Verify(source, serializeSigInput(beacon, instance, round, host.NetworkName()), ticket) == nil
}

const DOMAIN_SEPARATION_TAG_VRF = "VRF"

// Serializes the input to the VRF signature for the CONVERGE step of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func serializeSigInput(beacon []byte, instance uint64, round uint64, networkName NetworkName) []byte {
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
