package signing_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/signing"
	"github.com/stretchr/testify/suite"
)

func TestFake(t *testing.T) { suite.Run(t, NewSigningTestSuite(signing.NewFakeBackend())) }
