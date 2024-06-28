package emulator

import (
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

// Assertions is a utility wrapper around require.Assertions with the API
// tailored to asserting GPBFT interactions in order to reduce boilerplate code
// during testing.
type Assertions struct {
	*require.Assertions
}

// NewAssertions instantiates a new Assertions utility.
func NewAssertions(t *testing.T) *Assertions {
	return &Assertions{
		Assertions: require.New(t),
	}
}

// Decided asserts that the given instance has decided the expected value.
func (r *Assertions) Decided(instance *Instance, expect gpbft.ECChain) {
	decision := instance.GetDecision()
	r.NotNil(decision)
	r.Equal(expect, decision.Vote.Value)
}
