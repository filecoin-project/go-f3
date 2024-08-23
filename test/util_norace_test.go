//go:build !race

package test

import "testing"

func SkipInRaceMode(*testing.T) {
}
