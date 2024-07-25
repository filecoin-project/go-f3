package ec

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	mbase "github.com/multiformats/go-multibase"
)

var _ Backend = (*FakeEC)(nil)

type FakeEC struct {
	clock             clock.Clock
	useTime           bool
	seed              []byte
	initialPowerTable gpbft.PowerEntries

	// with time
	ecPeriod time.Duration
	ecStart  time.Time

	lk sync.RWMutex

	// without time
	currentHead int64

	// with time
	pausedAt *time.Time
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

func (ts *Tipset) String() string {
	res, _ := mbase.Encode(mbase.Base32, ts.tsk[:gpbft.CidMaxLen])
	for i := 1; i*gpbft.CidMaxLen < len(ts.tsk); i++ {
		enc, _ := mbase.Encode(mbase.Base32, ts.tsk[gpbft.CidMaxLen*i:gpbft.CidMaxLen*(i+1)])
		res += "," + enc
	}
	return res
}

func NewFakeEC(ctx context.Context, seed uint64, bootstrapEpoch int64, ecPeriod time.Duration, initialPowerTable gpbft.PowerEntries, useTime bool) *FakeEC {
	clk := clock.GetClock(ctx)
	return &FakeEC{
		clock:             clk,
		useTime:           useTime,
		seed:              binary.BigEndian.AppendUint64(nil, seed),
		initialPowerTable: initialPowerTable,

		ecPeriod: ecPeriod,
		ecStart:  clk.Now().Add(-time.Duration(bootstrapEpoch) * ecPeriod),
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
		tsk = append(tsk, gpbft.DigestToCid(digest)...)
	}
	return &Tipset{
		tsk:       tsk,
		epoch:     epoch,
		timestamp: ec.ecStart.Add(time.Duration(epoch) * ec.ecPeriod),
	}
}

func (ec *FakeEC) currentEpoch() int64 {
	if !ec.useTime {
		panic("only call this when use-time is true")
	}
	ec.lk.RLock()
	defer ec.lk.RUnlock()
	if ec.pausedAt != nil {
		return int64(ec.pausedAt.Sub(ec.ecStart) / ec.ecPeriod)
	}

	return int64(ec.clock.Since(ec.ecStart) / ec.ecPeriod)
}

// GetTipsetByHeight should return a tipset or nil/empty byte array if it does not exists
func (ec *FakeEC) GetTipsetByEpoch(ctx context.Context, epoch int64) (TipSet, error) {
	if ec.useTime && ec.currentEpoch() < epoch {
		return nil, fmt.Errorf("does not yet exist")
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
			return nil, fmt.Errorf("walking back tipsets: %w", err)
		}
		if ts != nil {
			return ts, nil
		}
	}
	return nil, fmt.Errorf("parent not found")
}

// SetCurrentHead sets the current head epoch.
// This is only supported by FakeEC if `useTime=false`
func (ec *FakeEC) SetCurrentHead(head int64) {
	if ec.useTime {
		panic("setting head only makes sense with manual EC, not use time is set to true")
	}
	ec.lk.Lock()
	ec.currentHead = head
	ec.lk.Unlock()
}

func (ec *FakeEC) GetCurrentHead() int64 {
	if ec.useTime {
		return ec.currentEpoch()
	}
	ec.lk.RLock()
	defer ec.lk.RUnlock()
	return ec.currentHead
}

// Pause pauses EC when `useTime=true`. Panics if `useTime=false`.
func (ec *FakeEC) Pause() {
	if !ec.useTime {
		panic("pausing only makes sense with time-based EC")
	}
	ec.lk.Lock()
	defer ec.lk.Unlock()

	t := ec.clock.Now()
	ec.pausedAt = &t
}

// Resume resumes EC when `useTime=true`. Panics if `useTime=false`.
func (ec *FakeEC) Resume() {
	if !ec.useTime {
		panic("pausing only makes sense with time-based EC")
	}
	ec.lk.Lock()
	defer ec.lk.Unlock()

	ec.pausedAt = nil
}

func (ec *FakeEC) GetHead(ctx context.Context) (TipSet, error) {
	if ec.useTime {
		return ec.GetTipsetByEpoch(ctx, ec.currentEpoch())
	}

	return ec.GetTipsetByEpoch(ctx, ec.GetCurrentHead())
}

func (ec *FakeEC) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	return ec.initialPowerTable, nil
}

func (ec *FakeEC) GetTipset(ctx context.Context, tsk gpbft.TipSetKey) (TipSet, error) {
	epoch := binary.BigEndian.Uint64(tsk[6+32-8 : 6+32])
	return ec.genTipset(int64(epoch)), nil
}
