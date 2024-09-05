package sim

import (
	"cmp"
	"context"
	"math/rand"
	"slices"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/signing"
)

// Generate a justification from the given power table. This assumes the signing backend can sign for all keys.
func MakeJustification(backend signing.Backend, nn gpbft.NetworkName, chain gpbft.ECChain, instance uint64, powerTable, nextPowerTable gpbft.PowerEntries) (*gpbft.Justification, error) {

	scaledPowerTable, totalPower, err := powerTable.Scaled()
	if err != nil {
		return nil, err
	}

	powerTableCid, err := certs.MakePowerTableCID(nextPowerTable)
	if err != nil {
		return nil, err
	}

	payload := gpbft.Payload{
		Instance: instance,
		Round:    0,
		Phase:    gpbft.DECIDE_PHASE,
		SupplementalData: gpbft.SupplementalData{
			PowerTable: powerTableCid,
		},
		Value: chain,
	}
	msg := backend.MarshalPayloadForSigning(nn, &payload)
	signers := rand.Perm(len(powerTable))
	signersBitfield := bitfield.New()
	var signingPower int64

	type vote struct {
		index int
		sig   []byte
		pk    gpbft.PubKey
	}

	var votes []vote
	for _, i := range signers {
		pe := powerTable[i]
		sig, err := backend.Sign(context.Background(), pe.PubKey, msg)
		if err != nil {
			return nil, err
		}
		votes = append(votes, vote{
			index: i,
			sig:   sig,
			pk:    pe.PubKey,
		})

		signersBitfield.Set(uint64(i))
		signingPower += scaledPowerTable[i]
		if gpbft.IsStrongQuorum(signingPower, totalPower) {
			break
		}
	}
	slices.SortFunc(votes, func(a, b vote) int {
		return cmp.Compare(a.index, b.index)
	})
	pks := make([]gpbft.PubKey, len(votes))
	sigs := make([][]byte, len(votes))
	for i, vote := range votes {
		pks[i] = vote.pk
		sigs[i] = vote.sig
	}

	sig, err := backend.Aggregate(pks, sigs)
	if err != nil {
		return nil, err
	}

	return &gpbft.Justification{
		Vote:      payload,
		Signers:   signersBitfield,
		Signature: sig,
	}, nil
}
