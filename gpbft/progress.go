package gpbft

import "sync/atomic"

var (
	_ ProgressObserver = (*atomicProgression)(nil)
	_ Progress         = (*atomicProgression)(nil).Get
)

// Progress gets the latest GPBFT instance progress.
type Progress func() (instant Instant)

// ProgressObserver defines an interface for observing and being notified about
// the progress of a GPBFT instance as it advances through different instance,
// rounds or phases.
type ProgressObserver interface {
	// NotifyProgress is called to notify the observer about the progress of GPBFT
	// instance, round or phase.
	NotifyProgress(instant Instant)
}

type atomicProgression struct {
	progression atomic.Pointer[Instant]
}

func newAtomicProgression() *atomicProgression {
	return &atomicProgression{}
}

func (a *atomicProgression) NotifyProgress(instant Instant) {
	a.progression.Store(&instant)
}

func (a *atomicProgression) Get() (instant Instant) {
	if latest := a.progression.Load(); latest != nil {
		instant = *latest
	}
	return
}
