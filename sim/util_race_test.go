//go:build race

package sim_test

import "testing"

func SkipInRaceMode(t *testing.T) {
	t.Helper()
	t.Skip("skipping in -race mode")
}
