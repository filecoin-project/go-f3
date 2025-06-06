package gpbft

import (
	"errors"
	"fmt"
	"runtime/debug"
)

var (
	_ error = (*ValidationError)(nil)

	// ErrValidationTooOld signals that a message is invalid because belongs to prior
	// instances of gpbft.
	ErrValidationTooOld = newValidationError("message is for prior instance")
	// ErrValidationNoCommittee signals that a message is invalid because there is no
	// committee for the instance to which it belongs.
	//
	// See: CommitteeProvider.
	ErrValidationNoCommittee = newValidationError("no committee for instance")
	// ErrValidationInvalid signals that a message violates the validity rules of
	// gpbft protocol.
	ErrValidationInvalid = newValidationError("message invalid")
	// ErrValidationWrongBase signals that a message is invalid due to having an
	// unexpected base ECChain.
	//
	// See: ECChain, TipSet, ECChain.Base
	ErrValidationWrongBase = newValidationError("unexpected base chain")
	//ErrValidationWrongSupplement signals that a message is invalid due to unexpected supplemental data.
	//
	// See SupplementalData.
	ErrValidationWrongSupplement = newValidationError("unexpected supplemental data")
	// ErrValidationNotRelevant signals that a message is not relevant at the current
	// instance, and is not worth propagating to others.
	ErrValidationNotRelevant = newValidationError("message is valid but not relevant")

	// ErrReceivedWrongInstance signals that a message is received with mismatching instance ID.
	ErrReceivedWrongInstance = errors.New("received message for wrong instance")
	// ErrReceivedAfterTermination signals that a message is received after the gpbft instance is terminated.
	ErrReceivedAfterTermination = errors.New("received message after terminating")
	// ErrReceivedInternalError signals that an error has occurred during message processing.
	ErrReceivedInternalError = errors.New("error processing message")
)

// ValidationError signals that an error has occurred while validating a GMessage.
type ValidationError struct{ message string }

type PanicError struct {
	Cause      any
	stackTrace string
}

func newValidationError(message string) ValidationError { return ValidationError{message: message} }
func (e ValidationError) Error() string                 { return e.message }

func newPanicError(cause any) *PanicError {
	return &PanicError{
		Cause:      cause,
		stackTrace: string(debug.Stack()),
	}
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("participant panicked: %v\n%v", e.Cause, e.stackTrace)
}
