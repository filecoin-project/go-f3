package mhelper

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/otel/attribute"
)

var (
	AttrStatus        = attribute.Key("status")
	AttrStatusSuccess = attribute.KeyValue{
		Key:   AttrStatus,
		Value: attribute.StringValue("success"),
	}
	AttrStatusError = attribute.KeyValue{
		Key:   AttrStatus,
		Value: attribute.StringValue("error-other"),
	}
	AttrStatusCanceled = attribute.KeyValue{
		Key:   AttrStatus,
		Value: attribute.StringValue("error-canceled"),
	}
	AttrStatusTimeout = attribute.KeyValue{
		Key:   AttrStatus,
		Value: attribute.StringValue("error-timeout"),
	}
	AttrStatusInternalError = attribute.KeyValue{
		Key:   AttrStatus,
		Value: attribute.StringValue("error-internal"),
	}

	AttrDialSucceeded = attribute.Key("dial-succeded")
)

func Status(ctx context.Context, err error) attribute.KeyValue {
	if err == nil {
		return AttrStatusSuccess
	}

	if os.IsTimeout(err) || errors.Is(err, os.ErrDeadlineExceeded) {
		return AttrStatusTimeout
	}

	switch ctx.Err() {
	case nil:
		return AttrStatusError
	case context.DeadlineExceeded:
		return AttrStatusTimeout
	default:
		return AttrStatusCanceled
	}
}
