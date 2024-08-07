package msgdump

import (
	"github.com/filecoin-project/go-f3/gpbft"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type GMessageEnvelopeDeffered struct {
	UnixMicroTime uint64
	NetworkName   string
	Message       cbg.Deferred
}
type GMessageEnvelope struct {
	UnixMicroTime uint64
	NetworkName   string
	Message       gpbft.GMessage
}
