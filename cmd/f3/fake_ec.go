package main

import (
	"context"
	"encoding/binary"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
)

type FakeEC struct {
	seed              []byte
	currentEpoch      func() int64
	initialPowerTable gpbft.PowerEntries
}

type Tipset struct {
	tsk   []byte
	epoch int64
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

func NewFakeEC(seed uint64, currentEpoch func() int64, initialPowerTable gpbft.PowerEntries) *FakeEC {
	return &FakeEC{
		seed:              binary.BigEndian.AppendUint64(nil, seed),
		currentEpoch:      currentEpoch,
		initialPowerTable: initialPowerTable,
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
	tsk := make([]byte, 0, size*gpbft.CID_MAX_LEN)

	if size == 0 {
		return nil
	}

	for i := uint8(0); i < size; i++ {
		h.Write([]byte{1})
		digest := h.Sum(nil)
		if i == 0 {
			//encode epoch in the first block hash
			binary.BigEndian.PutUint64(digest, uint64(epoch))
		}
		tsk = append(tsk, gpbft.DigestToCid(digest)...)
	}
	return &Tipset{
		tsk:   tsk,
		epoch: epoch,
	}
}

// GetTipsetByHeight should return a tipset or nil/empty byte array if it does not exists
func (ec *FakeEC) GetTipsetByEpoch(ctx context.Context, epoch int64) (f3.TipSet, error) {
	if ec.currentEpoch() < epoch {
		return nil, xerrors.Errorf("does not yet exist")
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
			return nil, xerrors.Errorf("walking back tipsets: %w", err)
		}
		if ts != nil {
			return ts, nil
		}
	}
	return nil, xerrors.Errorf("parent not found")
}

func (ec *FakeEC) GetHead(ctx context.Context) (f3.TipSet, error) {
	return ec.genTipset(ec.currentEpoch()), nil
}

func (ec *FakeEC) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return ec.initialPowerTable, nil
}

func (ec *FakeEC) GetTipset(ctx context.Context, tsk gpbft.TipSetKey) (f3.TipSet, error) {
	epoch := binary.BigEndian.Uint64(tsk[6 : 6+8])
	return ec.genTipset(int64(epoch)), nil
}
