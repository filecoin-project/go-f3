package measurements

import (
	"context"
	"errors"
	"os"

	"github.com/ipfs/go-datastore"
	"go.opentelemetry.io/otel/attribute"
)

var (
	AttrStatusSuccess       = attribute.String("status", "success")
	AttrStatusError         = attribute.String("status", "error-other")
	AttrStatusPanic         = attribute.String("status", "error-panic")
	AttrStatusCanceled      = attribute.String("status", "error-canceled")
	AttrStatusTimeout       = attribute.String("status", "error-timeout")
	AttrStatusInternalError = attribute.String("status", "error-internal")
	AttrStatusNotFound      = attribute.String("status", "error-not-found")

	AttrDialSucceeded = attribute.Key("dial-succeeded")
)

func Status(ctx context.Context, err error) attribute.KeyValue {
	switch cErr := ctx.Err(); {
	case err == nil:
		return AttrStatusSuccess
	case errors.Is(err, datastore.ErrNotFound):
		return AttrStatusNotFound
	case os.IsTimeout(err),
		errors.Is(err, os.ErrDeadlineExceeded),
		errors.Is(cErr, context.DeadlineExceeded):
		return AttrStatusTimeout
	case errors.Is(cErr, context.Canceled):
		return AttrStatusCanceled
	default:
		return AttrStatusError
	}
}
