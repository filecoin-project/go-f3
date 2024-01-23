package blssignatures

import (
	"fmt"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/f3"
)

type BLSSigner struct {
	suite      pairing.Suite
	keyGroup   kyber.Group
	privKey    kyber.Scalar
	powerTable *f3.PowerTable
}

func NewBLSSigner(privKeyData []byte, powerTable *f3.PowerTable) (*BLSSigner, error) {

	// This implementation is forced to use the group G2 for keys, because the underlying implementation
	// of the bdn package is hard-coded to use such a configuration.
	// It should be rather easy to make it support keys on G1 though.
	// Then, this code can very easily be generalized as well.
	suite := bls12381.NewBLS12381Suite()
	keyGroup := suite.G2()

	// Deserialize private key.
	privKey := keyGroup.Scalar()
	err := privKey.UnmarshalBinary(privKeyData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	return &BLSSigner{
		suite:      suite,
		keyGroup:   keyGroup,
		privKey:    privKey,
		powerTable: powerTable,
	}, nil
}

func (s *BLSSigner) Sign(msg []byte) ([]byte, error) {
	return bdn.Sign(s.suite, s.privKey, msg)
}

func (s *BLSSigner) Verify(signer f3.ActorID, msg, sig []byte) error {

	// Get sender's public key from power table.
	pubKey, err := lookUpPubKeyById(s.keyGroup, s.powerTable, signer)
	if err != nil {
		return err
	}

	return bdn.Verify(s.suite, pubKey, msg, sig)
}

func lookUpPubKeyById(keyGroup kyber.Group, powerTable *f3.PowerTable, signer f3.ActorID) (kyber.Point, error) {
	signerIndex, ok := powerTable.Lookup[signer]
	if !ok {
		return nil, fmt.Errorf("unknown signer: %v", signer)
	}

	pubKey := keyGroup.Point()
	err := pubKey.UnmarshalBinary(powerTable.Entries[signerIndex].PubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal public key at index %d", signerIndex)
	}
	return pubKey, nil

}
