//go:build !race
// +build !race

package test

import "testing"

func SkipInRaceMode(_ *testing.T) {
}
