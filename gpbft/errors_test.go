package gpbft

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidationError_SentinelValues(t *testing.T) {
	tests := []struct {
		name    string
		subject error
	}{
		{name: "ErrValidationTooOld", subject: ErrValidationTooOld},
		{name: "ErrValidationNoCommittee", subject: ErrValidationNoCommittee},
		{name: "ErrValidationInvalid", subject: ErrValidationInvalid},
		{name: "ErrValidationWrongBase", subject: ErrValidationWrongBase},
		{name: "ErrValidationWrongSupplement", subject: ErrValidationWrongSupplement},
		{name: "ErrValidationNotRelevant", subject: ErrValidationNotRelevant},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.True(t, errors.As(test.subject, &ValidationError{}))
			require.True(t, errors.As(test.subject, &ValidationError{message: "fish"}))
		})
	}
}
