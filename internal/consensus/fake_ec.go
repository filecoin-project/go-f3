package consensus

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/ec"
	"golang.org/x/crypto/blake2b"

	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_              ec.Backend = (*FakeEC)(nil)
	cidPrefixBytes            = gpbft.CidPrefix.Bytes()
)

type FakeEC struct {
	*fakeECOptions

	ecStart time.Time

	lk                sync.RWMutex
	pausedAt          *time.Time
	finalizedEpochs   map[int64]int64
	maxFinalizedEpoch int64
}

func NewFakeEC(o ...FakeECOption) *FakeEC {
	fec := &FakeEC{
		fakeECOptions:   newFakeECOptions(o...),
		finalizedEpochs: make(map[int64]int64),
	}
	fec.ecStart = fec.clock.Now().Add(-time.Duration(fec.bootstrapEpoch) * fec.ecPeriod)
	return fec
}

// randFloat returns random float in 0..1 range
func randFloat(rng []byte) float64 {
	bits := binary.BigEndian.Uint64(rng[:8])
	return float64(bits>>(64-52)) / float64(math.MaxUint64>>(64-52))
}

func (ec *FakeEC) genTipset(epoch int64) *tipset {
	seed := ec.getTipsetGenSeed(epoch)
	h, err := blake2b.New256(binary.BigEndian.AppendUint64(nil, uint64(seed)))
	if err != nil {
		panic(err)
	}
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(epoch)))
	rng := h.Sum(nil)
	var isZero bool
	isZero, rng = randFloat(rng) < ec.nullTipsetProbability, rng[8:]
	if isZero {
		return nil
	}
	size := (rng[0] % 7) + 1
	tsk := make([]byte, 0, size*gpbft.CidMaxLen)

	if size == 0 {
		return nil
	}

	for i := uint8(0); i < size; i++ {
		h.Write([]byte{1})
		digest := h.Sum(nil)
		if i == 0 {
			// Encode epoch in the first block hash along with the ID of this fake EC.
			binary.BigEndian.PutUint64(digest[32-8:], uint64(epoch))
			binary.BigEndian.PutUint64(digest[32-8-8:32-8], uint64(seed))
		}
		tsk = append(tsk, cidPrefixBytes...)
		tsk = append(tsk, digest...)
	}

	h.Reset()
	h.Write([]byte(fmt.Sprintf("beacon %d", epoch)))
	beacon := h.Sum(nil)

	return &tipset{
		tsk:       tsk,
		epoch:     epoch,
		timestamp: ec.ecStart.Add(time.Duration(epoch) * ec.ecPeriod),
		beacon:    beacon,
	}
}

func (ec *FakeEC) getTipsetGenSeed(epoch int64) int64 {
	if ec.forkAfterEpochs <= 0 {
		// The forking capability is disabled. Simply return whatever seed configured for
		// ec.
		return ec.seed
	}
	if epoch <= ec.bootstrapEpoch {
		// Generate a consistent chain up to the bootstrap epoch for the same bootstrap
		// epoch and ec period. Because, F3 bootstrap epoch is assumed to be final by
		// design.
		return ec.seed
	}

	// Find the seed based on what has been finalized so far.
	ec.lk.RLock()
	defer ec.lk.RUnlock()
	if epoch > ec.forkAfterEpochs+ec.maxFinalizedEpoch {
		// The epoch is beyond the fork after epoch, so we can fork. Change the seed to whatever
		return ec.forkSeed
	}
	epochs := make([]int64, 0, len(ec.finalizedEpochs))
	for finalizedEpoch := range ec.finalizedEpochs {
		epochs = append(epochs, finalizedEpoch)
	}
	slices.Sort(epochs)
	for _, e := range epochs {
		if e >= epoch {
			return ec.finalizedEpochs[e]
		}
	}

	return ec.seed
}

// GetTipsetByEpoch returns the tipset at a given epoch. If the epoch does not
// yet exist, it returns an error.
func (ec *FakeEC) GetTipsetByEpoch(_ context.Context, epoch int64) (ec.TipSet, error) {
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

func (ec *FakeEC) GetPowerTable(_ context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	targetEpoch, _ := ec.epochAndSeedFromTsk(tsk)
	headEpoch := ec.GetCurrentHead()
	if targetEpoch > headEpoch {
		return nil, fmt.Errorf("requested epoch %d beyond head %d", targetEpoch, headEpoch)
	}

	if ec.ecMaxLookback > 0 && targetEpoch < headEpoch-ec.ecMaxLookback {
		return nil, fmt.Errorf("oops, we forgot that power table, head %d, epoch %d", headEpoch, targetEpoch)
	}
	pt := ec.initialPowerTable
	if ec.evolvePowerTable != nil {
		pt = ec.evolvePowerTable(targetEpoch, pt)
	}
	return pt, nil
}

func (ec *FakeEC) epochAndSeedFromTsk(tsk gpbft.TipSetKey) (int64, int64) {
	return int64(binary.BigEndian.Uint64(tsk[6+32-8 : 6+32])), int64(binary.BigEndian.Uint64(tsk[6+32-8-8 : 6+32-8]))
}

func (ec *FakeEC) GetTipset(_ context.Context, tsk gpbft.TipSetKey) (ec.TipSet, error) {
	// Ignore the seed embedded in tipset key since it will be inferred to the right
	// one by genTipset depending on finalized tipests.
	epoch, _ := ec.epochAndSeedFromTsk(tsk)
	return ec.genTipset(epoch), nil
}

func (ec *FakeEC) Finalize(_ context.Context, tsk gpbft.TipSetKey) error {
	epoch, seed := ec.epochAndSeedFromTsk(tsk)
	ec.lk.Lock()
	defer ec.lk.Unlock()
	if foundSeed, found := ec.finalizedEpochs[epoch]; found && seed != foundSeed {
		return fmt.Errorf("epoch already finalized: %d %d", epoch, seed)
	}
	ec.finalizedEpochs[epoch] = seed
	ec.maxFinalizedEpoch = max(epoch, ec.maxFinalizedEpoch)
	return nil
}
