package sim_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/test"
)

func TestBLSSigning(t *testing.T) {
	fakeSig := &sim.FakeSigner{}
	test.NewSigningSuite(fakeSig, fakeSig).Run(t)
}
