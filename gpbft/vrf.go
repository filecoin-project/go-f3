package gpbft

import (
	"bytes"
	"context"
	"encoding/binary"
)

// A ticket is a signature over some common payload.
type Ticket []byte

const DomainSeparationTagVRF = "VRF"

func MakeTicket(ctx context.Context, nn NetworkName, beacon []byte, instance uint64, round uint64, source PubKey, signer Signer) (Ticket, error) {
	return signer.Sign(ctx, source, vrfSerializeSigInput(beacon, instance, round, nn))
}

func VerifyTicket(nn NetworkName, beacon []byte, instance uint64, round uint64, source PubKey, verifier Verifier, ticket Ticket) bool {
	return verifier.Verify(source, vrfSerializeSigInput(beacon, instance, round, nn), ticket) == nil
}

// Serializes the input to the VRF signature for the CONVERGE step of GossiPBFT.
// Only used for VRF ticket creation and/or verification.
func vrfSerializeSigInput(beacon []byte, instance uint64, round uint64, networkName NetworkName) []byte {
	var buf bytes.Buffer

	buf.WriteString(DomainSeparationTagVRF)
	buf.WriteString(":")
	buf.WriteString(string(networkName))
	buf.WriteString(":")
	buf.Write(beacon)
	buf.WriteString(":")
	_ = binary.Write(&buf, binary.BigEndian, instance)
	_ = binary.Write(&buf, binary.BigEndian, round)

	return buf.Bytes()
}
