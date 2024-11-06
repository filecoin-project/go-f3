package observer

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var emptyCommitments [32]byte

type message struct {
	Timestamp     time.Time      `json:"Timestamp"`
	NetworkName   string         `json:"NetworkName"`
	Sender        gpbft.ActorID  `json:"Sender"`
	Vote          payload        `json:"Vote"`
	Signature     []byte         `json:"Signature"`
	Ticket        []byte         `json:"Ticket"`
	Justification *justification `json:"Justification"`
}

type justification struct {
	Vote      payload  `json:"Vote"`
	Signers   []uint64 `json:"Signers"`
	Signature []byte   `json:"Signature"`
}

type payload struct {
	Instance         uint64           `json:"Instance"`
	Round            uint64           `json:"Round"`
	Phase            string           `json:"Phase"`
	SupplementalData supplementalData `json:"SupplementalData"`
	Value            []tipSet         `json:"Value"`
}

type supplementalData struct {
	Commitments []byte `json:"Commitments"`
	PowerTable  string `json:"PowerTable"`
}

type tipSet struct {
	Epoch       int64  `json:"Epoch"`
	Key         []byte `json:"Key"`
	Commitments []byte `json:"Commitments"`
	PowerTable  string `json:"PowerTable"`
}

func newMessage(timestamp time.Time, nn string, msg gpbft.GMessage) (*message, error) {
	j, err := newJustification(msg.Justification)
	if err != nil {
		return nil, err
	}
	return &message{
		Timestamp:     timestamp,
		NetworkName:   nn,
		Sender:        msg.Sender,
		Signature:     msg.Signature,
		Ticket:        msg.Ticket,
		Vote:          newPayload(msg.Vote),
		Justification: j,
	}, nil
}

func newJustification(gj *gpbft.Justification) (*justification, error) {
	if gj == nil {
		return nil, nil
	}
	const maxSingers = 1 << 16
	signers, err := gj.Signers.All(maxSingers)
	if err != nil {
		return nil, err
	}
	return &justification{
		Vote:      newPayload(gj.Vote),
		Signers:   signers,
		Signature: gj.Signature,
	}, nil
}

func newPayload(gp gpbft.Payload) payload {
	var commitments []byte
	if gp.SupplementalData.Commitments != emptyCommitments {
		// Currently, all Commitments are always empty. For completeness and reducing
		// future schema changes include them anyway when they are non-empty.
		commitments = gp.SupplementalData.Commitments[:]
	}

	value := make([]tipSet, len(gp.Value))
	for i, v := range gp.Value {
		value[i] = tipSet{
			Epoch:      v.Epoch,
			Key:        v.Key,
			PowerTable: v.PowerTable.String(),
		}
		if v.Commitments != emptyCommitments {
			value[i].Commitments = v.Commitments[:]
		}
	}
	return payload{
		Instance: gp.Instance,
		Round:    gp.Round,
		Phase:    gp.Phase.String(),
		SupplementalData: supplementalData{
			Commitments: commitments,
			PowerTable:  gp.SupplementalData.PowerTable.String(),
		},
		Value: value,
	}
}
