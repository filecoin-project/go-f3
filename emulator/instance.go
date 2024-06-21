package emulator

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
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
// See Driver.Start.
func NewInstance(t *testing.T, id uint64, powerEntries gpbft.PowerEntries, proposal ...gpbft.TipSet) *Instance {
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
	require.NoError(t, err)
	pt := gpbft.NewPowerTable()
	require.NoError(t, pt.Add(powerEntries...))
	for i, tipset := range proposal {
		if len(tipset.PowerTable) == 0 {
			// Populate missing power table CIDs to avoid validation error when constructing
			// ECChain.
			proposal[i].PowerTable = ptCid
		}
	}
	if len(proposal) < 1 {
		require.Fail(t, "at least one proposal tipset must be specified")
	}
	proposalChain, err := gpbft.NewChain(proposal[0], proposal[1:]...)
	require.NoError(t, err)
	return &Instance{
		id:         id,
		powerTable: pt,
		beacon:     []byte(fmt.Sprintf("🥓%d", id)),
		proposal:   proposalChain,
	}
}

func (i *Instance) Proposal() gpbft.ECChain                  { return i.proposal }
func (i *Instance) GetDecision() *gpbft.Justification        { return i.decision }
func (i *Instance) ID() uint64                               { return i.id }
func (i *Instance) SupplementalData() gpbft.SupplementalData { return i.supplementalData }

func (i *Instance) NewQuality(proposal gpbft.ECChain) gpbft.Payload {
	return i.NewPayload(0, gpbft.QUALITY_PHASE, proposal)
}

func (i *Instance) NewPrepare(round uint64, proposal gpbft.ECChain) gpbft.Payload {
	return i.NewPayload(round, gpbft.PREPARE_PHASE, proposal)
}

func (i *Instance) NewCommit(round uint64, proposal gpbft.ECChain) gpbft.Payload {
	return i.NewPayload(round, gpbft.COMMIT_PHASE, proposal)
}

func (i *Instance) NewConverge(round uint64, proposal gpbft.ECChain) gpbft.Payload {
	return i.NewPayload(round, gpbft.CONVERGE_PHASE, proposal)
}

func (i *Instance) NewDecide(round uint64, proposal gpbft.ECChain) gpbft.Payload {
	return i.NewPayload(round, gpbft.DECIDE_PHASE, proposal)
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

func (d *Driver) NewJustification(round uint64, step gpbft.Phase, vote gpbft.ECChain, from ...gpbft.ActorID) *gpbft.Justification {
	payload := gpbft.Payload{
		Instance:         d.currentInstance.ID(),
		Round:            round,
		Step:             step,
		SupplementalData: d.currentInstance.SupplementalData(),
		Value:            vote,
	}
	msg := signing.MarshalPayloadForSigning(d.host.NetworkName(), &payload)
	qr := gpbft.QuorumResult{
		Signers:    make([]int, len(from)),
		PubKeys:    make([]gpbft.PubKey, len(from)),
		Signatures: make([][]byte, len(from)),
	}
	for i, actor := range from {
		index, found := d.currentInstance.powerTable.Lookup[actor]
		d.require.True(found)
		entry := d.currentInstance.powerTable.Entries[index]
		signature, err := signing.Sign(entry.PubKey, msg)
		d.require.NoError(err)
		qr.Signatures[i] = signature
		qr.PubKeys[i] = entry.PubKey
		qr.Signers[i] = index
	}
	aggregate, err := signing.Aggregate(qr.PubKeys, qr.Signatures)
	d.require.NoError(err)
	return &gpbft.Justification{
		Vote:      payload,
		Signers:   qr.SignersBitfield(),
		Signature: aggregate,
	}
}
