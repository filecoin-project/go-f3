package adversary

import (
	"math/rand"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ Receiver = (*Drop)(nil)

// Drop adversary stochastically drops messages to/from a given set of
// participants for a configured duration of time, mimicking at-most-once message
// delivery semantics across a simulation network.
//
// When no participants are set, all exchanged messages will be targeted by this
// adversary. For this adversary to take effect global stabilisation time must be
// configured to be at least as long as the configured drop duration.
//
// See  sim.WithGlobalStabilizationTime.
type Drop struct {
	id              gpbft.ActorID
	host            Host
	targetsByID     map[gpbft.ActorID]struct{}
	gst             time.Time
	rng             *rand.Rand
	dropProbability float64
}

func NewDrop(id gpbft.ActorID, host Host, seed int64, dropProbability float64, dropDuration time.Duration, targets ...gpbft.ActorID) *Drop {
	targetsByID := make(map[gpbft.ActorID]struct{})
	for _, target := range targets {
		targetsByID[target] = struct{}{}
	}
	return &Drop{
		id:              id,
		host:            host,
		rng:             rand.New(rand.NewSource(seed)),
		dropProbability: dropProbability,
		targetsByID:     targetsByID,
		gst:             time.Time{}.Add(dropDuration),
	}
}

func NewDropGenerator(power *gpbft.StoragePower, seed int64, dropProbability float64, dropDuration time.Duration, targets ...gpbft.ActorID) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewDrop(id, host, seed, dropProbability, dropDuration, targets...),
			Power:    power,
		}
	}
}

func (d *Drop) ID() gpbft.ActorID {
	return d.id
}

func (d *Drop) AllowMessage(from gpbft.ActorID, to gpbft.ActorID, _ gpbft.GMessage) bool {
	// Stochastically drop messages until Global Stabilisation Time has
	// elapsed, except messages to self.
	switch {
	case from == to, d.host.Time().After(d.gst), !d.isTargeted(to) && !d.isTargeted(from):
		return true
	default:
		return d.allowStochastically()
	}
}

func (d *Drop) allowStochastically() bool {
	switch {
	case d.dropProbability <= 0:
		return true
	case d.dropProbability >= 1.0:
		return false
	default:
		return d.rng.Float64() > d.dropProbability
	}
}

func (d *Drop) isTargeted(id gpbft.ActorID) bool {
	if len(d.targetsByID) == 0 {
		// Target all participants if no explicit IDs are set.
		return true
	}
	_, found := d.targetsByID[id]
	return found
}

func (*Drop) ValidateMessage(msg *gpbft.GMessage) (gpbft.ValidatedMessage, error) {
	return Validated(msg), nil
}

func (*Drop) ReceiveMessage(gpbft.ValidatedMessage) error { return nil }
func (*Drop) ReceiveAlarm() error                         { return nil }
func (*Drop) StartInstanceAt(uint64, time.Time) error     { return nil }
