package f3

import "context"

// These methods are restricted to tests only because they can interfere with the manifest sender
// logic in strange ways (and aren't something we want to support long-term anyways).

// Resume the F3 runner. Internal method for testing only.
func (m *F3) Resume(ctx context.Context) error {
	return m.startInternal(ctx)
}

// Pause the F3 runner. Internal method for testing only.
func (m *F3) Pause(ctx context.Context) error {
	return m.stopInternal(ctx)
}
