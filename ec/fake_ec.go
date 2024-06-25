package ec

import (
	"context"
	"encoding/binary"
	"time"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-f3/gpbft"
)

type FakeEC struct {
	useTime           bool
	seed              []byte
	initialPowerTable gpbft.PowerEntries

	// with time
	ecPeriod time.Duration
	ecStart  time.Time

	// without time
	currentHead int64
}

type Tipset struct {
	tsk       []byte
	epoch     int64
	timestamp time.Time
}

func (ts *Tipset) Key() gpbft.TipSetKey {
	return ts.tsk
}

func (ts *Tipset) Epoch() int64 {
	return ts.epoch
}
func (ts *Tipset) Beacon() []byte {
	h, err := blake2b.New256([]byte("beacon"))
	if err != nil {
		panic(err)
	}
	h.Write(ts.tsk)
	return h.Sum(nil)
}

func (ts *Tipset) Timestamp() time.Time {
	return ts.timestamp
}

func NewFakeEC(seed uint64, bootstrapEpoch int64, ecPeriod time.Duration, initialPowerTable gpbft.PowerEntries) *FakeEC {
	return &FakeEC{
		useTime:           true,
		seed:              binary.BigEndian.AppendUint64(nil, seed),
		initialPowerTable: initialPowerTable,

		ecPeriod: ecPeriod,
		ecStart:  time.Now().Add(-time.Duration(bootstrapEpoch) * ecPeriod),
	}
}

func NewFakeECWithoutTime(seed uint64, bootstrapEpoch int64, ecPeriod time.Duration, initialPowerTable gpbft.PowerEntries) *FakeEC {
	fec := NewFakeEC(seed, bootstrapEpoch, ecPeriod, initialPowerTable)
	fec.useTime = false
	fec.currentHead = bootstrapEpoch
	return fec
}

func (ec *FakeEC) genTipset(epoch int64) *Tipset {
	h, err := blake2b.New256(ec.seed)
	if err != nil {
		panic(err)
	}
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(epoch)))
	rng := h.Sum(nil)
	var size uint8
	size, rng = rng[0]%8, rng[1:]
	_ = rng
	tsk := make([]byte, 0, size*gpbft.CID_MAX_LEN)

	if size == 0 {
		return nil
	}

	for i := uint8(0); i < size; i++ {
		h.Write([]byte{1})
		digest := h.Sum(nil)
		if i == 0 {
			//encode epoch in the first block hash
			binary.BigEndian.PutUint64(digest[32-8:], uint64(epoch))
		}
		tsk = append(tsk, gpbft.DigestToCid(digest)...)
	}
	return &Tipset{
		tsk:       tsk,
		epoch:     epoch,
		timestamp: ec.ecStart.Add(time.Duration(epoch) * ec.ecPeriod),
	}
}

func (ec *FakeEC) currentEpoch() int64 {
	return int64(time.Since(ec.ecStart) / ec.ecPeriod)
}

// GetTipsetByHeight should return a tipset or nil/empty byte array if it does not exists
func (ec *FakeEC) GetTipsetByEpoch(ctx context.Context, epoch int64) (TipSet, error) {
	if ec.useTime && ec.currentEpoch() < epoch {
		return nil, xerrors.Errorf("does not yet exist")
	}
	ts := ec.genTipset(epoch)
	for ts == nil {
		epoch--
		ts = ec.genTipset(epoch)
	}
	return ts, nil
}

func (ec *FakeEC) GetParent(ctx context.Context, ts TipSet) (TipSet, error) {

	for epoch := ts.Epoch() - 1; epoch > 0; epoch-- {
		ts, err := ec.GetTipsetByEpoch(ctx, epoch)
		if err != nil {
			return nil, xerrors.Errorf("walking back tipsets: %w", err)
		}
		if ts != nil {
			return ts, nil
		}
	}
	return nil, xerrors.Errorf("parent not found")
}

func (ec *FakeEC) SetCurrentHead(head int64) {
	ec.currentHead = head
}

func (ec *FakeEC) GetCurrentHead() int64 {
	return ec.currentHead
}

func (ec *FakeEC) GetHead(ctx context.Context) (TipSet, error) {
	if ec.useTime {
		return ec.GetTipsetByEpoch(ctx, ec.currentEpoch())
	}

	return ec.GetTipsetByEpoch(ctx, ec.currentHead)
}

func (ec *FakeEC) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return ec.initialPowerTable, nil
}

func (ec *FakeEC) GetTipset(ctx context.Context, tsk gpbft.TipSetKey) (TipSet, error) {
	epoch := binary.BigEndian.Uint64(tsk[6+32-8 : 6+32])
	return ec.genTipset(int64(epoch)), nil
}
