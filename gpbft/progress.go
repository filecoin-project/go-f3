package gpbft

import "sync/atomic"

var (
	_ ProgressObserver = (*atomicProgression)(nil)
	_ Progress         = (*atomicProgression)(nil).Get
)

type InstanceProgress struct {
	Instant
	// Input is the initial input chain to the instance. This field may be nil in a
	// case where the instance is scheduled to start but has not started yet.
	// Because, the input chain is only determined once the start alarm triggers.
	//
	// See: Participant.StartInstanceAt.
	Input *ECChain
}

// Progress gets the latest GPBFT instance progress.
type Progress func() InstanceProgress

// ProgressObserver defines an interface for observing and being notified about
// the progress of a GPBFT instance as it advances through different instance,
// rounds or phases.
type ProgressObserver interface {
	// NotifyProgress is called to notify the observer about the progress of GPBFT
	// instance, round or phase.
	NotifyProgress(instant InstanceProgress)
}

type atomicProgression struct {
	progression atomic.Pointer[InstanceProgress]
}

func newAtomicProgression() *atomicProgression {
	return &atomicProgression{}
}

func (a *atomicProgression) NotifyProgress(instant InstanceProgress) {
	a.progression.Store(&instant)
}

func (a *atomicProgression) Get() (instant InstanceProgress) {
	if latest := a.progression.Load(); latest != nil {
		instant = *latest
	}
	return
}
