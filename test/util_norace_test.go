//go:build !race
// +build !race

package test

import "testing"

func SkipInRaceMode(*testing.T) {
}
