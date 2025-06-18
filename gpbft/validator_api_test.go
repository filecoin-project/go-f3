package gpbft

import "github.com/filecoin-project/go-f3/internal/caching"

type Validator interface {
	MessageValidator
	PartialMessageValidator
}

// NewValidator creates a new Validator instance with the provided parameters for
// testing purposes.
func NewValidator(nn NetworkName, verifier Verifier, cp CommitteeProvider, progress Progress, cache *caching.GroupedSet, committeeLookback uint64) Validator {
	return newValidator(nn, verifier, cp, progress, cache, committeeLookback)
}
