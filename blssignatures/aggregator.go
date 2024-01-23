package blssignatures

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/drand/kyber/sign"
	"github.com/drand/kyber/sign/bdn"
	"github.com/filecoin-project/go-f3/f3"
)

// SigAggregator holds the intermediate state of signature aggregation.
// Must not be allocated directly. Always use AggSignatureScheme.NewAggregator to create a new instance.
type SigAggregator struct {
	blsScheme  *BLSAggScheme
	sigs       []indexedSig
	signerMask *sign.Mask
}

// Add adds a signature to the aggregate. This implementation does not compute the aggregate signature right away.
// That computation is performed only by the Aggregate method.
// Progressive computation of the aggregate as the individual signatures are added would probably require
// modifications (optimizations) in the github.com/drand/kyber/sign/bdn implementation.
func (as *SigAggregator) Add(signer f3.ActorID, sig []byte) error {

	signerIndex, ok := as.blsScheme.powerTable.Lookup[signer]
	if !ok {
		return fmt.Errorf("signer %v not in power table", signer)
	}
	if err := as.signerMask.SetBit(signerIndex, true); err != nil {
		return err
	}

	// We store the power table index along with each signature, because the bdn.AggregateSignatures library function
	// expects the signatures to be ordered the same way as the signers in the mask.
	// Thus, before aggregation, this slice will be ordered by index.
	// An alternative would be to keep the signatures ordered by signer index at all times.
	as.sigs = append(as.sigs, indexedSig{
		index: signerIndex,
		sig:   sig,
	})

	return nil
}

// Aggregate aggregates all the signatures previously added using the Add method.
// The returned byte slice is an opaque encoding of the signature itself,
// along with references to the identities of the signers.
// The returned data can be passed to BLSAggScheme.VerifyAggSig for verification.
// Note that the verifying BLSAggScheme must have been created using the same power table
// as the BLSAggScheme that produced this aggregator. Otherwise, the verification result is undefined.
func (as *SigAggregator) Aggregate() ([]byte, error) {

	// Since the bdn.AggregateSignatures library function expects the signatures to be ordered
	// the same way as the signers in the mask, we need to sort as.sigs first.
	// An alternative would be to keep the signatures ordered by signer index at all times
	// (and thus maintain the ordering at each signature addition).
	sort.Slice(as.sigs, func(i, j int) bool {
		return as.sigs[i].index < as.sigs[j].index
	})
	sigs := make([][]byte, len(as.sigs))
	for i, sig := range as.sigs {
		sigs[i] = sig.sig
	}

	// Aggregate signatures.
	var data aggSigData
	aggSig, err := bdn.AggregateSignatures(as.blsScheme.suite, sigs, as.signerMask)
	if err != nil {
		return nil, fmt.Errorf("failed to compute aggregate signature: %w", err)
	}

	// Serialize and return the signature and the signers' bitmask.
	if data.Sig, err = aggSig.MarshalBinary(); err != nil {
		return nil, fmt.Errorf("failed marshaling signature data: %w", err)
	}
	data.Mask = as.signerMask.Mask()
	return json.Marshal(data)
}
