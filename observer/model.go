package observer

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-bitfield"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/ipfs/go-cid"
)

const maxSingers = 1 << 16

var emptyCommitments [32]byte

type Message struct {
	Timestamp     time.Time      `json:"Timestamp"`
	NetworkName   string         `json:"NetworkName"`
	Sender        gpbft.ActorID  `json:"Sender"`
	Vote          Payload        `json:"Vote"`
	Signature     []byte         `json:"Signature"`
	Ticket        []byte         `json:"Ticket"`
	Justification *Justification `json:"Justification"`
	VoteValueKey  []byte         `json:"VoteValueKey"`
}

type Justification struct {
	Vote      Payload  `json:"Vote"`
	Signers   []uint64 `json:"Signers"`
	Signature []byte   `json:"Signature"`
}

type Payload struct {
	Instance         uint64           `json:"Instance"`
	Round            uint64           `json:"Round"`
	Phase            string           `json:"Phase"`
	SupplementalData SupplementalData `json:"SupplementalData"`
	Value            []TipSet         `json:"Value"`
}

type SupplementalData struct {
	Commitments []byte `json:"Commitments"`
	PowerTable  string `json:"PowerTable"`
}

type TipSet struct {
	Epoch       int64  `json:"Epoch"`
	Key         []byte `json:"Key"`
	Commitments []byte `json:"Commitments"`
	PowerTable  string `json:"PowerTable"`
}

type FinalityCertificate struct {
	Timestamp        time.Time         `json:"Timestamp"`
	NetworkName      string            `json:"NetworkName"`
	Instance         uint64            `json:"Instance"`
	ECChain          []TipSet          `json:"ECChain"`
	SupplementalData SupplementalData  `json:"SupplementalData"`
	Signers          []uint64          `json:"Signers"`
	Signature        []byte            `json:"Signature"`
	PowerTableDelta  []PowerTableDelta `json:"PowerTableDelta"`
}

type PowerTableDelta struct {
	ParticipantID uint64 `json:"ParticipantID"`
	PowerDelta    int64  `json:"PowerDelta"`
	SigningKey    []byte `json:"SigningKey"`
}

type ChainExchange struct {
	Timestamp    time.Time `json:"Timestamp"`
	NetworkName  string    `json:"NetworkName"`
	Instance     uint64    `json:"Instance"`
	VoteValueKey []byte    `json:"VoteValueKey"`
	VoteValue    []TipSet  `json:"VoteValue"`
}

func newMessage(timestamp time.Time, nn string, msg gpbft.PartialGMessage) (*Message, error) {
	j, err := newJustification(msg.Justification)
	if err != nil {
		return nil, err
	}
	return &Message{
		Timestamp:     timestamp,
		NetworkName:   nn,
		Sender:        msg.Sender,
		Signature:     msg.Signature,
		Ticket:        msg.Ticket,
		Vote:          newPayload(msg.Vote),
		Justification: j,
		VoteValueKey:  msg.VoteValueKey[:],
	}, nil
}

func newJustification(gj *gpbft.Justification) (*Justification, error) {
	if gj == nil {
		return nil, nil
	}
	signers, err := gj.Signers.All(maxSingers)
	if err != nil {
		return nil, err
	}
	return &Justification{
		Vote:      newPayload(gj.Vote),
		Signers:   signers,
		Signature: gj.Signature,
	}, nil
}

func newPayload(gp gpbft.Payload) Payload {
	return Payload{
		Instance:         gp.Instance,
		Round:            gp.Round,
		Phase:            gp.Phase.String(),
		SupplementalData: supplementalDataFromGpbft(gp.SupplementalData),
		Value:            tipsetsFromGpbft(gp.Value),
	}
}

func tipsetsFromGpbft(chain *gpbft.ECChain) []TipSet {
	value := make([]TipSet, chain.Len())
	for i, v := range chain.TipSets {
		value[i] = TipSet{
			Epoch:      v.Epoch,
			Key:        v.Key,
			PowerTable: v.PowerTable.String(),
		}
		if v.Commitments != emptyCommitments {
			value[i].Commitments = v.Commitments[:]
		}
	}
	return value
}

func supplementalDataFromGpbft(sd gpbft.SupplementalData) SupplementalData {
	var commitments []byte
	if sd.Commitments != emptyCommitments {
		// Currently, all Commitments are always empty. For completeness and reducing
		// future schema changes include them anyway when they are non-empty.
		commitments = sd.Commitments[:]
	}
	return SupplementalData{
		Commitments: commitments,
		PowerTable:  sd.PowerTable.String(),
	}
}

func (m Message) ToPartialMessage() (*gpbft.PartialGMessage, error) {
	payload, err := m.Vote.ToGpbftPayload()
	if err != nil {
		return nil, fmt.Errorf("could not convert vote to gpbft payload: %w", err)
	}
	gj, err := m.Justification.ToGpbftJustification()
	if err != nil {
		return nil, fmt.Errorf("could not convert justification to gpbft justification: %w", err)
	}

	return &gpbft.PartialGMessage{
		GMessage: &gpbft.GMessage{
			Sender:        m.Sender,
			Vote:          payload,
			Signature:     m.Signature,
			Ticket:        m.Ticket,
			Justification: gj,
		},
		VoteValueKey: gpbft.ECChainKey(m.VoteValueKey),
	}, nil
}

func (p Payload) ToGpbftPayload() (gpbft.Payload, error) {
	phase, err := phaseFromString(p.Phase)
	if err != nil {
		return gpbft.Payload{}, fmt.Errorf("could not convert phase: %w", err)
	}
	sd, err := p.SupplementalData.ToGpbftSupplementalData()
	if err != nil {
		return gpbft.Payload{}, fmt.Errorf("could not convert supplemental data: %w", err)
	}

	gv, err := ToGpbftChain(p.Value...)
	if err != nil {
		return gpbft.Payload{}, fmt.Errorf("could not convert value: %w", err)
	}

	return gpbft.Payload{
		Instance:         p.Instance,
		Round:            p.Round,
		Phase:            phase,
		SupplementalData: sd,
		Value:            gv,
	}, nil
}

func (s SupplementalData) ToGpbftSupplementalData() (gpbft.SupplementalData, error) {
	var sd gpbft.SupplementalData
	if len(s.Commitments) > 0 {
		copy(sd.Commitments[:], s.Commitments)
	}
	var err error
	sd.PowerTable, err = cid.Decode(s.PowerTable)
	if err != nil {
		return sd, fmt.Errorf("failed to decode PowerTable CID: %w", err)
	}
	return sd, nil
}

func (t TipSet) ToGpbftTipSet() (*gpbft.TipSet, error) {
	sup, err := SupplementalData{
		Commitments: t.Commitments,
		PowerTable:  t.PowerTable,
	}.ToGpbftSupplementalData()
	if err != nil {
		return nil, err
	}
	return &gpbft.TipSet{
		Epoch:       t.Epoch,
		Key:         t.Key,
		PowerTable:  sup.PowerTable,
		Commitments: sup.Commitments,
	}, nil
}

func ToGpbftChain(tss ...TipSet) (*gpbft.ECChain, error) {
	if len(tss) == 0 {
		return nil, nil
	}
	gtss := make([]*gpbft.TipSet, len(tss))
	var err error
	for i, ts := range tss {
		gtss[i], err = ts.ToGpbftTipSet()
		if err != nil {
			return nil, err
		}
	}
	return gpbft.NewChain(gtss[0], gtss[1:]...)
}

func (j *Justification) ToGpbftJustification() (*gpbft.Justification, error) {
	if j == nil {
		return nil, nil
	}
	ri, _ := rlepluslazy.RunsFromSlice(j.Signers)
	gsigners, _ := bitfield.NewFromIter(ri)
	payload, err := j.Vote.ToGpbftPayload()
	if err != nil {
		return nil, fmt.Errorf("could not convert vote to gpbft payload: %w", err)
	}

	return &gpbft.Justification{
		Vote:      payload,
		Signers:   gsigners,
		Signature: j.Signature,
	}, nil
}

func phaseFromString(phase string) (gpbft.Phase, error) {
	switch phase {
	case "INITIAL":
		return gpbft.INITIAL_PHASE, nil
	case "QUALITY":
		return gpbft.QUALITY_PHASE, nil
	case "CONVERGE":
		return gpbft.CONVERGE_PHASE, nil
	case "PREPARE":
		return gpbft.PREPARE_PHASE, nil
	case "COMMIT":
		return gpbft.COMMIT_PHASE, nil
	case "DECIDE":
		return gpbft.DECIDE_PHASE, nil
	case "TERMINATED":
		return gpbft.TERMINATED_PHASE, nil
	default:
		return 0, fmt.Errorf("unknown phase: %s", phase)
	}
}

func newFinalityCertificate(timestamp time.Time, nn gpbft.NetworkName, cert *certs.FinalityCertificate) (*FinalityCertificate, error) {
	if cert == nil {
		return nil, fmt.Errorf("cannot create FinalityCertificate with nil cert")
	}

	signers, err := cert.Signers.All(maxSingers)
	if err != nil {
		return nil, fmt.Errorf("failed to list finality certificate signers: %w", err)
	}

	deltas, err := powerTableDeltaFromGpbft(cert.PowerTableDelta)
	if err != nil {
		return nil, fmt.Errorf("failed to convert finality certificate power table delta: %w", err)
	}

	return &FinalityCertificate{
		Timestamp:        timestamp,
		NetworkName:      string(nn),
		Instance:         cert.GPBFTInstance,
		ECChain:          tipsetsFromGpbft(cert.ECChain),
		SupplementalData: supplementalDataFromGpbft(cert.SupplementalData),
		Signers:          signers,
		Signature:        cert.Signature,
		PowerTableDelta:  deltas,
	}, nil
}

func powerTableDeltaFromGpbft(ptd certs.PowerTableDiff) ([]PowerTableDelta, error) {

	deltas := make([]PowerTableDelta, len(ptd))
	for i, delta := range ptd {
		deltas[i] = PowerTableDelta{
			ParticipantID: uint64(delta.ParticipantID),
			PowerDelta:    delta.PowerDelta.Int64(),
			SigningKey:    delta.SigningKey,
		}
	}
	return deltas, nil
}

func newChainExchange(timestamp time.Time, nn gpbft.NetworkName, instance uint64, chain *gpbft.ECChain) *ChainExchange {
	key := chain.Key()
	return &ChainExchange{
		Timestamp:    timestamp,
		NetworkName:  string(nn),
		Instance:     instance,
		VoteValueKey: key[:],
		VoteValue:    tipsetsFromGpbft(chain),
	}
}
