package gpbft

import "sync/atomic"

var (
	_ ProgressObserver = (*atomicProgression)(nil)
	_ Progress         = (*atomicProgression)(nil).Get
)

// Progress gets the latest GPBFT instance progress.
type Progress func() (instance, round uint64, phase Phase)

// ProgressObserver defines an interface for observing and being notified about
// the progress of a GPBFT instance as it advances through different instance,
// rounds or phases.
type ProgressObserver interface {
	// NotifyProgress is called to notify the observer about the progress of GPBFT
	// instance, round or phase.
	NotifyProgress(instance, round uint64, phase Phase)
}

type atomicProgression struct {
	progression atomic.Pointer[progress]
}

type progress struct {
	instance uint64
	round    uint64
	phase    Phase
}

func newAtomicProgression() *atomicProgression {
	return &atomicProgression{}
}

func (a *atomicProgression) NotifyProgress(instance, round uint64, phase Phase) {
	a.progression.Store(&progress{
		instance: instance,
		round:    round,
		phase:    phase,
	})
}

func (a *atomicProgression) Get() (instance, round uint64, phase Phase) {
	if latest := a.progression.Load(); latest != nil {
		instance, round, phase = latest.instance, latest.round, latest.phase
	}
	return
}
