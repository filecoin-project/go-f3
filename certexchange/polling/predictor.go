package polling

import (
	"time"
)

const maxBackoffMultiplier = 10

func newPredictor(minInterval, defaultInterval, maxInterval time.Duration) *predictor {
	return &predictor{
		minInterval:     minInterval,
		maxInterval:     maxInterval,
		interval:        defaultInterval,
		exploreDistance: defaultInterval / 2,
	}
}

// An interval predictor that tries to predict the time between instances. It can't predict the time
// an instance will be available, but it'll keep adjusting the interval until we receive one
// instance per interval.
type predictor struct {
	minInterval, maxInterval time.Duration

	interval        time.Duration
	wasIncreasing   bool
	exploreDistance time.Duration

	backoff time.Duration
}

// Update the predictor. The one argument indicates how many certificates we received since the last
// update.
//
// - 2+ -> interval is too long.
// - 1 -> interval is perfect.
// - 0 -> interval is too short.
//
// We don't actually know the _offset_... but whatever. We can keep up +/- one instance and that's
// fine (especially because of the power table lag, etc.).
func (p *predictor) update(progress uint64) time.Duration {
	if p.backoff > 0 {
		if progress > 0 {
			p.backoff = 0
		}
	} else if progress != 1 {
		// If we've made too much progress (interval too long) or made no progress (interval
		// too short), explore to find the right interval.

		if p.wasIncreasing == (progress > 1) {
			// We switched directions which means we're circling the target, shrink the
			// explore distance.
			p.exploreDistance /= 3
		} else if progress <= 2 {
			// We repeatedly made no progress, or slightly too much progress. Double the
			// explore distance.
			p.exploreDistance *= 2
		} else {
			// We far away from our target and aren't polling often enough. Reset to a
			// sane estimate.
			p.interval /= time.Duration(progress)
			p.exploreDistance = p.interval / 2
		}

		// Make sure the explore distance doesn't get too short/long.
		if p.exploreDistance < p.minInterval/100 {
			p.exploreDistance = p.minInterval / 100
		} else if p.exploreDistance > p.maxInterval/2 {
			p.exploreDistance = p.maxInterval / 2
		}

		// Then update the interval.
		if progress == 0 {
			// If we fail to make progress, enter "backoff" mode. We'll leave backoff
			// mode next time we receive a certificate. Otherwise, we'd end up quickly
			// skewing our belief of the correct interval e.g., due to a skipped
			// instance.
			p.backoff = p.interval
			p.interval += p.exploreDistance
			p.wasIncreasing = true
		} else {
			p.interval -= p.exploreDistance
			p.wasIncreasing = false
		}

		// Clamp between min/max
		if p.interval < p.minInterval {
			p.interval = p.minInterval
		} else if p.interval > p.maxInterval {
			p.interval = p.maxInterval
		}
	}

	// Apply either the backoff or predicted the interval.
	nextInterval := p.interval
	if p.backoff > 0 {
		nextInterval = p.backoff
		p.backoff = min(2*p.backoff, maxBackoffMultiplier*p.maxInterval)
	}
	return nextInterval

}
