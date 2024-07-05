//go:build race
// +build race

package test

import "testing"

func SkipInRaceMode(t *testing.T) {
	t.Skip("skipping in -race mode")
}
