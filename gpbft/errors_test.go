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
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			require.True(t, errors.As(test.subject, &ValidationError{}))
			require.True(t, errors.As(test.subject, &ValidationError{message: "fish"}))
		})
	}
}
