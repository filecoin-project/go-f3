package emulator

import (
	"errors"
	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_ gpbft.Verifier         = (*nilSigning)(nil)
	_ gpbft.Signer           = (*nilSigning)(nil)
	_ gpbft.SigningMarshaler = (*nilSigning)(nil)

	signing nilSigning
)

// TODO: allow explicit setting of values to return from methods, to simulate good/bad signatures.
// Optionally allow setting expectations of the inputs to Sign/Aggregate
type nilSigning struct{}

func (s nilSigning) Sign(sender gpbft.PubKey, msg []byte) ([]byte, error) {
	return []byte("signed"), nil
}

func (s nilSigning) Verify(sender gpbft.PubKey, msg, got []byte) error {
	return nil
}

func (s nilSigning) Aggregate(signers []gpbft.PubKey, sigs [][]byte) ([]byte, error) {
	if len(signers) != len(sigs) {
		return nil, errors.New("public keys and signatures length mismatch")
	}
	return []byte("aggregated"), nil
}

func (s nilSigning) VerifyAggregate(payload, got []byte, signers []gpbft.PubKey) error {
	return nil
}

func (s nilSigning) MarshalPayloadForSigning(name gpbft.NetworkName, payload *gpbft.Payload) []byte {
	return []byte("marshalled")
}
