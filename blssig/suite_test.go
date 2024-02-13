package blssig_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/test"
)

func TestBLSSigning(t *testing.T) {
	test.NewSigningSuite(
		blssig.SignerWithKeyOnG2(),
		blssig.VerifierWithKeyOnG2()).Run(t)
}
