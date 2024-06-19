package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
)

type FakeEC struct {
	seed              []byte
	initialPowerTable gpbft.PowerEntries

	ecPeriod time.Duration
	ecStart  time.Time
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

func NewFakeEC(seed uint64, m f3.Manifest) *FakeEC {
	return &FakeEC{
		seed:              binary.BigEndian.AppendUint64(nil, seed),
		initialPowerTable: m.InitialPowerTable,

		ecPeriod: m.ECPeriod,
		ecStart:  m.ECBoostrapTimestamp,
	}
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
func (ec *FakeEC) GetTipsetByEpoch(ctx context.Context, epoch int64) (f3.TipSet, error) {
	if ec.currentEpoch() < epoch {
		return nil, fmt.Errorf("does not yet exist")
	}
	ts := ec.genTipset(epoch)
	for ts == nil {
		epoch--
		ts = ec.genTipset(epoch - 1)
	}
	return ts, nil
}

func (ec *FakeEC) GetParent(ctx context.Context, ts f3.TipSet) (f3.TipSet, error) {

	for epoch := ts.Epoch() - 1; epoch > 0; epoch-- {
		ts, err := ec.GetTipsetByEpoch(ctx, epoch)
		if err != nil {
			return nil, fmt.Errorf("walking back tipsets: %w", err)
		}
		if ts != nil {
			return ts, nil
		}
	}
	return nil, fmt.Errorf("parent not found")
}

func (ec *FakeEC) GetHead(ctx context.Context) (f3.TipSet, error) {
	return ec.GetTipsetByEpoch(ctx, ec.currentEpoch())
}

func (ec *FakeEC) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return ec.initialPowerTable, nil
}

func (ec *FakeEC) GetTipset(ctx context.Context, tsk gpbft.TipSetKey) (f3.TipSet, error) {
	epoch := binary.BigEndian.Uint64(tsk[6+32-8 : 6+32])
	return ec.genTipset(int64(epoch)), nil
}
