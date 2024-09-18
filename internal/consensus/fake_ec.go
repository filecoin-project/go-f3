package consensus

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"golang.org/x/crypto/blake2b"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	mbase "github.com/multiformats/go-multibase"
)

var (
	_ ec.Backend = (*FakeEC)(nil)
	_ ec.TipSet  = (*tipset)(nil)
)

type FakeEC struct {
	clock             clock.Clock
	seed              []byte
	initialPowerTable gpbft.PowerEntries

	ecPeriod time.Duration
	ecStart  time.Time

	lk       sync.RWMutex
	pausedAt *time.Time
}

type tipset struct {
	tsk       []byte
	epoch     int64
	timestamp time.Time
}

func (ts *tipset) Key() gpbft.TipSetKey {
	return ts.tsk
}

func (ts *tipset) Epoch() int64 {
	return ts.epoch
}
func (ts *tipset) Beacon() []byte {
	h, err := blake2b.New256([]byte("beacon"))
	if err != nil {
		panic(err)
	}
	h.Write(ts.tsk)
	return h.Sum(nil)
}

func (ts *tipset) Timestamp() time.Time {
	return ts.timestamp
}

func (ts *tipset) String() string {
	res, _ := mbase.Encode(mbase.Base32, ts.tsk[:gpbft.CidMaxLen])
	for i := 1; i*gpbft.CidMaxLen < len(ts.tsk); i++ {
		enc, _ := mbase.Encode(mbase.Base32, ts.tsk[gpbft.CidMaxLen*i:gpbft.CidMaxLen*(i+1)])
		res += "," + enc
	}
	return res
}

func NewFakeEC(ctx context.Context, seed uint64, bootstrapEpoch int64, ecPeriod time.Duration, initialPowerTable gpbft.PowerEntries) *FakeEC {
	clk := clock.GetClock(ctx)
	return &FakeEC{
		clock:             clk,
		seed:              binary.BigEndian.AppendUint64(nil, seed),
		initialPowerTable: initialPowerTable,

		ecPeriod: ecPeriod,
		ecStart:  clk.Now().Add(-time.Duration(bootstrapEpoch) * ecPeriod),
	}
}

var cidPrefixBytes = gpbft.CidPrefix.Bytes()

func (ec *FakeEC) genTipset(epoch int64) *tipset {
	h, err := blake2b.New256(ec.seed)
	if err != nil {
		panic(err)
	}
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(epoch)))
	rng := h.Sum(nil)
	var size uint8
	size, rng = rng[0]%8, rng[1:]
	if size == 0 {
		// if tipset is empty, try again to reduce change for empty tipset
		// from 12.5% to 1.5%
		size = rng[0] % 8
	}
	tsk := make([]byte, 0, size*gpbft.CidMaxLen)

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
		tsk = append(tsk, cidPrefixBytes...)
		tsk = append(tsk, digest...)
	}
	return &tipset{
		tsk:       tsk,
		epoch:     epoch,
		timestamp: ec.ecStart.Add(time.Duration(epoch) * ec.ecPeriod),
	}
}

// GetTipsetByHeight should return a tipset or nil/empty byte array if it does not exists
func (ec *FakeEC) GetTipsetByEpoch(ctx context.Context, epoch int64) (ec.TipSet, error) {
	if ec.GetCurrentHead() < epoch {
		return nil, fmt.Errorf("does not yet exist")
	}
	ts := ec.genTipset(epoch)
	for ts == nil {
		epoch--
		ts = ec.genTipset(epoch)
	}
	return ts, nil
}

func (ec *FakeEC) GetParent(ctx context.Context, ts ec.TipSet) (ec.TipSet, error) {
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

func (ec *FakeEC) GetCurrentHead() int64 {
	ec.lk.RLock()
	defer ec.lk.RUnlock()
	if ec.pausedAt != nil {
		return int64(ec.pausedAt.Sub(ec.ecStart) / ec.ecPeriod)
	}

	return int64(ec.clock.Since(ec.ecStart) / ec.ecPeriod)
}

// Pause pauses EC.
func (ec *FakeEC) Pause() {
	ec.lk.Lock()
	defer ec.lk.Unlock()

	t := ec.clock.Now()
	ec.pausedAt = &t
}

// Resume resumes EC.
func (ec *FakeEC) Resume() {
	ec.lk.Lock()
	defer ec.lk.Unlock()

	ec.pausedAt = nil
}

func (ec *FakeEC) GetHead(ctx context.Context) (ec.TipSet, error) {
	return ec.GetTipsetByEpoch(ctx, ec.GetCurrentHead())
}

func (ec *FakeEC) GetPowerTable(context.Context, gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return ec.initialPowerTable, nil
}

func (ec *FakeEC) GetTipset(_ context.Context, tsk gpbft.TipSetKey) (ec.TipSet, error) {
	epoch := binary.BigEndian.Uint64(tsk[6+32-8 : 6+32])
	return ec.genTipset(int64(epoch)), nil
}

func (ec *FakeEC) Finalize(context.Context, gpbft.TipSetKey) error { return nil }
