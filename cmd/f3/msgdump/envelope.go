package msgdump

import (
	"github.com/filecoin-project/go-f3/gpbft"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type GMessageEnvelopeDeffered struct {
	UnixMicroTime int64
	NetworkName   string
	Message       cbg.Deferred
}
type GMessageEnvelope struct {
	UnixMicroTime int64
	NetworkName   string
	Message       gpbft.GMessage
}

type ParquetEnvelope struct {
	TimestampMicro int64 `parquet:"TimestampMicro,timestamp(microsecond)"`
	NetworkName    string
	Message        PMessage
}

type PMessage struct {
	Sender        gpbft.ActorID
	Vote          PPayload
	Signature     []byte
	Ticket        gpbft.Ticket
	Justification *PJustification
}

type PJustification struct {
	Vote      PPayload
	Signers   []uint64
	Signature []byte
}

type PPayload struct {
	Instance         uint64
	Round            uint64
	Phase            string `parquet:"Phase,enum"`
	SupplementalData gpbft.SupplementalData
	Value            gpbft.ECChain
}

func payloadToParquet(vote gpbft.Payload) PPayload {
	return PPayload{
		Instance:         vote.Instance,
		Round:            vote.Round,
		Phase:            vote.Phase.String(),
		SupplementalData: vote.SupplementalData,
		Value:            vote.Value,
	}
}

func ToParquet(gme GMessageEnvelope) (ParquetEnvelope, error) {
	pe := ParquetEnvelope{
		TimestampMicro: gme.UnixMicroTime,
		NetworkName:    gme.NetworkName,
		Message: PMessage{
			Sender:    gme.Message.Sender,
			Vote:      payloadToParquet(gme.Message.Vote),
			Signature: gme.Message.Signature,
			Ticket:    gme.Message.Ticket,
		},
	}
	if gme.Message.Justification != nil {
		signers, err := gme.Message.Justification.Signers.All(1 << 16)
		if err != nil {
			return ParquetEnvelope{}, err
		}

		pe.Message.Justification = &PJustification{
			Vote:      payloadToParquet(gme.Message.Justification.Vote),
			Signers:   signers,
			Signature: gme.Message.Justification.Signature,
		}
	}
	return pe, nil
}
