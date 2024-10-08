//go:build !race

package sim_test

import "testing"

func SkipInRaceMode(*testing.T) {
}
