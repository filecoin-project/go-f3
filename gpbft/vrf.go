package gpbft

import (
	"bytes"
	"encoding/binary"
)

// A ticket is a signature over some common payload.
type Ticket []byte

const DomainSeparationTagVRF = "VRF"

func VerifyTicket(nn NetworkName, beacon []byte, instance uint64, round uint64, source PubKey, verifier Verifier, ticket Ticket) bool {
	return verifier.Verify(source, vrfSerializeSigInput(beacon, instance, round, nn), ticket) == nil
}

// Serializes the input to the VRF signature for the CONVERGE phase of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func vrfSerializeSigInput(beacon []byte, instance uint64, round uint64, networkName NetworkName) []byte {
	const separator = ":"
	var buf bytes.Buffer
	buf.Grow(len(DomainSeparationTagVRF) +
		len(beacon) +
		len(networkName) +
		len(separator)*3 +
		16)

	buf.WriteString(DomainSeparationTagVRF)
	buf.WriteString(separator)
	buf.WriteString(string(networkName))
	buf.WriteString(separator)
	buf.Write(beacon)
	buf.WriteString(separator)
	_ = binary.Write(&buf, binary.BigEndian, instance)
	_ = binary.Write(&buf, binary.BigEndian, round)

	return buf.Bytes()
}
