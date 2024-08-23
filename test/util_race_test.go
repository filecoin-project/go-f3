//go:build race

package test

import "testing"

func SkipInRaceMode(t *testing.T) {
	t.Helper()
	t.Skip("skipping in -race mode")
}
