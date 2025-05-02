package measurements

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const attDsOperationKey = "operation"

var (
	_ datastore.Datastore = (*meteredDatastore)(nil)

	attrDsOperationGet     = attribute.String(attDsOperationKey, "get")
	attrDsOperationHas     = attribute.String(attDsOperationKey, "has")
	attrDsOperationGetSize = attribute.String(attDsOperationKey, "get-size")
	attrDsOperationQuery   = attribute.String(attDsOperationKey, "query")
	attrDsOperationPut     = attribute.String(attDsOperationKey, "put")
	attrDsOperationDelete  = attribute.String(attDsOperationKey, "delete")
	attrDsOperationSync    = attribute.String(attDsOperationKey, "sync")
	attrDsOperationClose   = attribute.String(attDsOperationKey, "close")
)

type meteredDatastore struct {
	delegate datastore.Datastore

	latency metric.Float64Histogram
	bytes   metric.Int64Histogram
}

// NewMeteredDatastore wraps the delegate with metrics, measuring latency and
// bytes exchanged depending on the operation.
func NewMeteredDatastore(meter metric.Meter, metricsPrefix string, delegate datastore.Datastore) datastore.Datastore {
	return &meteredDatastore{
		delegate: delegate,
		latency: Must(meter.Float64Histogram(
			fmt.Sprintf("%slatency", metricsPrefix),
			metric.WithDescription("The datastore latency labelled by operation and status."),
			metric.WithUnit("s"))),
		bytes: Must(meter.Int64Histogram(
			fmt.Sprintf("%sbytes", metricsPrefix),
			metric.WithDescription("The datastore exchanged bytes labelled by operation and status."),
			metric.WithUnit("By"))),
	}
}

func (m *meteredDatastore) Get(ctx context.Context, key datastore.Key) (_value []byte, _err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), len(_value), _err, attrDsOperationGet)
	}(time.Now())
	return m.delegate.Get(ctx, key)
}

func (m *meteredDatastore) Has(ctx context.Context, key datastore.Key) (_ bool, _err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), -1, _err, attrDsOperationHas)
	}(time.Now())
	return m.delegate.Has(ctx, key)
}

func (m *meteredDatastore) GetSize(ctx context.Context, key datastore.Key) (_size int, _err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), _size, _err, attrDsOperationGetSize)
	}(time.Now())
	return m.delegate.GetSize(ctx, key)
}

func (m *meteredDatastore) Query(ctx context.Context, q query.Query) (_ query.Results, _err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), -1, _err, attrDsOperationQuery)
	}(time.Now())
	return m.delegate.Query(ctx, q)
}

func (m *meteredDatastore) Put(ctx context.Context, key datastore.Key, value []byte) (_err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), len(value), _err, attrDsOperationPut)
	}(time.Now())
	return m.delegate.Put(ctx, key, value)
}

func (m *meteredDatastore) Delete(ctx context.Context, key datastore.Key) (_err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), -1, _err, attrDsOperationDelete)
	}(time.Now())
	return m.delegate.Delete(ctx, key)
}

func (m *meteredDatastore) Sync(ctx context.Context, prefix datastore.Key) (_err error) {
	defer func(start time.Time) {
		m.recordMetrics(ctx, time.Since(start), -1, _err, attrDsOperationSync)
	}(time.Now())
	return m.delegate.Sync(ctx, prefix)
}

func (m *meteredDatastore) Close() (_err error) {
	defer func(start time.Time) {
		m.recordMetrics(context.Background(), time.Since(start), -1, _err, attrDsOperationClose)
	}(time.Now())
	return m.delegate.Close()
}

func (m *meteredDatastore) recordMetrics(ctx context.Context, latency time.Duration, bytes int, err error, operation attribute.KeyValue) {
	attributes := metric.WithAttributes(operation, Status(ctx, err))
	m.latency.Record(ctx, latency.Seconds(), attributes)
	if bytes > -1 {
		m.bytes.Record(ctx, int64(bytes), attributes)
	}
}
