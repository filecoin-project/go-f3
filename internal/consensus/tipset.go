package consensus

import (
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	mbase "github.com/multiformats/go-multibase"
)

var _ ec.TipSet = (*tipset)(nil)

type tipset struct {
	tsk       []byte
	epoch     int64
	timestamp time.Time
	beacon    []byte
}

func (ts *tipset) Key() gpbft.TipSetKey { return ts.tsk }
func (ts *tipset) Epoch() int64         { return ts.epoch }
func (ts *tipset) Beacon() []byte       { return ts.beacon }
func (ts *tipset) Timestamp() time.Time { return ts.timestamp }

func (ts *tipset) String() string {
	res, _ := mbase.Encode(mbase.Base32, ts.tsk[:gpbft.CidMaxLen])
	for i := 1; i*gpbft.CidMaxLen < len(ts.tsk); i++ {
		enc, _ := mbase.Encode(mbase.Base32, ts.tsk[gpbft.CidMaxLen*i:gpbft.CidMaxLen*(i+1)])
		res += "," + enc
	}
	return res
}
