package adversary

import (
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var (
	_               Receiver           = (*Deny)(nil)
	DenyAllMessages DenyMessageMatcher = func(*gpbft.GMessage) bool { return true }
)

// Deny adversary denies all messages to/from a given set of participants for a
// configured duration of time.
//
// For this adversary to take effect, global stabilization time must be configured
// to be at least as long as the configured deny duration.
//
// See sim.WithGlobalStabilizationTime.
type Deny struct {
	host        Host
	targetsByID map[gpbft.ActorID]struct{}
	gst         time.Time
	msgMatched  DenyMessageMatcher
	mode        DenyTargetMode

	Absent
}

// DenyMessageMatcher checks whether a message should be denied by the Deny adversary or not.
//
// See: DenyAllMessages, DenyPhase.
type DenyMessageMatcher func(*gpbft.GMessage) bool

type DenyTargetMode int

func (m DenyTargetMode) String() string {
	switch m {
	case DenyToOrFrom:
		return "deny to or from"
	case DenyTo:
		return "deny to"
	case DenyFrom:
		return "deny from"
	default:
		panic("unknown case")
	}
}

const (
	// DenyToOrFrom denies message to or from target actor IDs.
	DenyToOrFrom DenyTargetMode = iota
	// DenyTo only denies messages destined to target actor IDs.
	DenyTo
	// DenyFrom only denies messages sent from target actor IDs.
	DenyFrom
)

// DenyPhase denies all messages at the given phase.
func DenyPhase(phase gpbft.Phase) DenyMessageMatcher {
	return func(message *gpbft.GMessage) bool {
		return message.Vote.Phase == phase
	}
}

func NewDeny(host Host, denialDuration time.Duration, msgMatcher DenyMessageMatcher, mode DenyTargetMode, targets ...gpbft.ActorID) *Deny {
	targetsByID := make(map[gpbft.ActorID]struct{})
	for _, target := range targets {
		targetsByID[target] = struct{}{}
	}
	return &Deny{
		host:        host,
		targetsByID: targetsByID,
		gst:         time.Time{}.Add(denialDuration),
		msgMatched:  msgMatcher,
		mode:        mode,
	}
}

func NewDenyGenerator(power gpbft.StoragePower, denialDuration time.Duration, msgMatcher DenyMessageMatcher, mode DenyTargetMode, targets ...gpbft.ActorID) Generator {
	return func(id gpbft.ActorID, host Host) *Adversary {
		return &Adversary{
			Receiver: NewDeny(host, denialDuration, msgMatcher, mode, targets...),
			Power:    power,
			ID:       id,
		}
	}
}

func (d *Deny) AllowMessage(from gpbft.ActorID, to gpbft.ActorID, msg gpbft.GMessage) bool {
	// Deny all messages to or from targets until Global Stabilisation Time has
	// elapsed, except messages to self.
	switch {
	case from == to, d.host.Time().After(d.gst):
		return true
	case d.mode == DenyTo:
		return !d.isTargeted(to, &msg)
	case d.mode == DenyFrom:
		return !d.isTargeted(from, &msg)
	case d.mode == DenyToOrFrom:
		return !(d.isTargeted(from, &msg) || d.isTargeted(to, &msg))
	default:
		panic("unexpected denial case")
	}
}

func (d *Deny) isTargeted(id gpbft.ActorID, msg *gpbft.GMessage) bool {
	switch _, found := d.targetsByID[id]; {
	case found:
		return d.msgMatched(msg)
	default:
		return false
	}
}
