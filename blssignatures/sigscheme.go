package blssignatures

import (
	"encoding/json"
	"fmt"

	"github.com/drand/kyber"
	bls12381 "github.com/drand/kyber-bls12381"
	"github.com/drand/kyber/pairing"
	"github.com/drand/kyber/sign"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/f3"
)

// BLSScheme represents a BLS aggregate signature scheme.
type BLSScheme struct {
	suite      pairing.Suite
	keyGroup   kyber.Group
	privKey    kyber.Scalar
	powerTable *f3.PowerTable
	pubKeys    []kyber.Point // deserialized public keys from the power table
}

// NewBLSScheme implements signing, verifying, and signature aggregation using BLS.
func NewBLSScheme(privKeyData []byte, powerTable *f3.PowerTable) (*BLSScheme, error) {

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

	// Deserialize public keys.
	pubKeys := make([]kyber.Point, len(powerTable.Entries))
	for i, entry := range powerTable.Entries {
		pubKey := keyGroup.Point()
		if err := pubKey.UnmarshalBinary(entry.PubKey); err != nil {
			return nil, fmt.Errorf("failed to unmarshal public key at index %d of the power table: %w", i, err)
		}
		pubKeys[i] = pubKey
	}

	return &BLSScheme{
		suite:      suite,
		keyGroup:   keyGroup,
		privKey:    privKey,
		powerTable: powerTable,
		pubKeys:    pubKeys,
	}, nil
}

func (s *BLSScheme) Sign(msg []byte) ([]byte, error) {
	return bdn.Sign(s.suite, s.privKey, msg)
}

func (s *BLSScheme) Verify(signer f3.ActorID, msg, sig []byte) error {

	// Get sender's public key from power table.
	pubKey, err := s.lookUpPubKeyById(signer)
	if err != nil {
		return err
	}

	return bdn.Verify(s.suite, pubKey, msg, sig)
}

func (s *BLSScheme) NewAggregator() (f3.SigAggregator, error) {
	mask, err := sign.NewMask(s.suite, s.pubKeys, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create empty public key mask: %w", err)
	}

	return &SigAggregator{
		blsScheme:  s,
		sigs:       make([]indexedSig, 0),
		signerMask: mask,
	}, nil
}

func (s *BLSScheme) VerifyAggSig(msg []byte, sig []byte) error {

	var data aggSigData
	if err := json.Unmarshal(sig, &data); err != nil {
		return fmt.Errorf("wrong signature data format: %w", err)
	}

	mask, err := sign.NewMask(s.suite, s.pubKeys, nil)
	if err != nil {
		return fmt.Errorf("failed to create empty public key mask: %w", err)
	}
	if err := mask.SetMask(data.Mask); err != nil {
		return fmt.Errorf("failed to load public key mask: %w", err)
	}

	// An aggregated signature is valid if it is a result of aggregating an empty set of individual signatures.
	if mask.CountEnabled() == 0 && len(data.Sig) == 0 {
		return nil
	}

	aggPubKey, err := bdn.AggregatePublicKeys(s.suite, mask)
	if err != nil {
		return fmt.Errorf("failed aggregating public keys")
	}

	return bdn.Verify(s.suite, aggPubKey, msg, data.Sig)
}

func (s *BLSScheme) lookUpPubKeyById(signer f3.ActorID) (kyber.Point, error) {
	signerIndex, ok := s.powerTable.Lookup[signer]
	if !ok {
		return nil, fmt.Errorf("unknown signer: %v", signer)
	}
	return s.lookUpPubKeyByIndex(signerIndex)
}

func (s *BLSScheme) lookUpPubKeyByIndex(signerIndex int) (kyber.Point, error) {
	pubKey := s.keyGroup.Point()
	err := pubKey.UnmarshalBinary(s.powerTable.Entries[signerIndex].PubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal public key at index %d", signerIndex)
	}
	return pubKey, nil

	// TODO: Cache deserialized public keys, so they don't have to be deserialized over and over again.
}
