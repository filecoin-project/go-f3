package emulator

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
)

// Instance represents a GPBFT instance capturing all the information necessary
// for GPBFT to function, along with the final decision reached if any.
type Instance struct {
	id               uint64
	supplementalData gpbft.SupplementalData
	proposal         gpbft.ECChain
	powerTable       *gpbft.PowerTable
	beacon           []byte
	decision         *gpbft.Justification
}

// NewInstance instantiates a new Instance for emulation. If absent, the
// constructor will implicitly generate any missing but required values such as
// public keys Power Table CID, etc. for the given params. The given proposal
// must contain at least one tipset.
//
// See Emulator.Start.
func NewInstance(id uint64, powerEntries gpbft.PowerEntries, proposal ...gpbft.TipSet) (*Instance, error) {
	// UX of the gpbft API is pretty painful; encapsulate the pain of getting an
	// instance going here at the price of accepting partial data and implicitly
	// filling what's missing.

	for i, entry := range powerEntries {
		if len(entry.PubKey) == 0 {
			// Populate missing public key to avoid power table validation errors.
			powerEntries[i].PubKey = []byte(fmt.Sprintf("🪪%d", entry.ID))
		}
	}
	ptCid, err := certs.MakePowerTableCID(powerEntries)
	if err != nil {
		return nil, err
	}
	pt := gpbft.NewPowerTable()
	if err := pt.Add(powerEntries...); err != nil {
		return nil, err
	}
	for i, tipset := range proposal {
		if len(tipset.PowerTable) == 0 {
			// Populate missing power table CIDs to avoid validation error when constructing
			// ECChain.
			proposal[i].PowerTable = ptCid
		}
	}
	if len(proposal) < 1 {
		return nil, errors.New("at least one proposal tipset must be specified")
	}
	proposalChain, err := gpbft.NewChain(proposal[0], proposal[1:]...)
	if err != nil {
		return nil, err
	}
	return &Instance{
		id:         id,
		powerTable: pt,
		beacon:     []byte(fmt.Sprintf("🥓%d", id)),
		proposal:   proposalChain,
	}, nil
}

func (i *Instance) GetProposal() gpbft.ECChain {
	return i.proposal
}
func (i *Instance) GetDecision() *gpbft.Justification {
	return i.decision
}

func (i *Instance) NewQualityForProposal() *gpbft.MessageBuilder {
	return i.NewQuality(i.GetProposal())
}

func (i *Instance) NewQuality(proposal gpbft.ECChain) *gpbft.MessageBuilder {
	return i.NewMessageBuilder(gpbft.Payload{
		Step:  gpbft.QUALITY_PHASE,
		Value: proposal,
	}, nil, false)
}

func (i *Instance) NewPrepareForProposal() *gpbft.MessageBuilder {
	return i.NewPrepare(i.GetProposal())
}

func (i *Instance) NewPrepare(proposal gpbft.ECChain) *gpbft.MessageBuilder {
	return i.NewMessageBuilder(gpbft.Payload{
		Step:  gpbft.PREPARE_PHASE,
		Value: proposal,
	}, nil, false)
}

func (i *Instance) NewMessageBuilder(payload gpbft.Payload, justification *gpbft.Justification, withTicket bool) *gpbft.MessageBuilder {

	payload.SupplementalData = i.supplementalData
	payload.Instance = i.id
	builder := gpbft.NewMessageBuilder(i.powerTable)
	builder.SetPayload(payload)
	if justification != nil {
		builder.SetJustification(justification)
	}
	if withTicket {
		builder.SetBeaconForTicket(i.beacon)
	}
	return builder
}

func (i *Instance) NewPayload(round uint64, step gpbft.Phase, value gpbft.ECChain) gpbft.Payload {
	return gpbft.Payload{
		Instance:         i.id,
		Round:            round,
		Step:             step,
		SupplementalData: i.supplementalData,
		Value:            value,
	}
}

func (i *Instance) NewJustification(payload gpbft.Payload, signerIndices []int, signatures ...[]byte) (*gpbft.Justification, error) {
	qr := gpbft.QuorumResult{
		Signers:    signerIndices,
		PubKeys:    make([]gpbft.PubKey, len(signerIndices)),
		Signatures: signatures,
	}
	for j, signerIndex := range signerIndices {
		entry := i.powerTable.Entries[signerIndex]
		qr.PubKeys[j] = entry.PubKey
	}
	aggregate, err := signing.Aggregate(qr.PubKeys, qr.Signatures)
	if err != nil {
		return nil, err
	}
	return &gpbft.Justification{
		Vote:      payload,
		Signers:   qr.SignersBitfield(),
		Signature: aggregate,
	}, nil
}
