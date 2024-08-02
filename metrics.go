package f3

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("f3")
var sampleSets = struct {
	messages       *sampleSet
	justifications *sampleSet
}{
	// The the max size of 25K is based on network size of ~3K and 5 phases of GPBFT,
	// where the vast majority of instances should complete in one round. That makes
	// up ~15K messages per instance. We may observe messages from previous or future
	// rounds, hence an additional capacity of 10K, making a total of 25K messages.
	//
	// Although, the number of observable messages with justification are expected to
	// be less than the total number of observed messages we use the same 25K bounded
	// size for justifications. The memory footprint of the additionally stored
	// samples is negligible in return for a larger sample size and ultimately a more
	// accurate measurement considering justification signature validation is more
	// expensive than message signature validation.
	//
	// Since sampleSet caps the max size of cache keys to 96 bytes (the length
	// of BLS signatures) the total memory footprint for 50K samples should be be
	// below 10MB (5MB * 2 due to bookkeeping overhead).
	messages:       newSampleSet(25_000),
	justifications: newSampleSet(25_000),
}

var metrics = struct {
	headDiverged                      metric.Int64Counter
	reconfigured                      metric.Int64Counter
	manifestsReceived                 metric.Int64Counter
	validationTime                    metric.Float64Histogram
	proposalFetchTime                 metric.Float64Histogram
	committeeFetchTime                metric.Float64Histogram
	validationDuplicateMessages       metric.Int64Counter
	validationDuplicateJustifications metric.Int64Counter
}{
	headDiverged:      must(meter.Int64Counter("f3_head_diverged", metric.WithDescription("Number of times we encountered the head has diverged from base scenario."))),
	reconfigured:      must(meter.Int64Counter("f3_reconfigured", metric.WithDescription("Number of times we reconfigured due to new manifest being delivered."))),
	manifestsReceived: must(meter.Int64Counter("f3_manifests_received", metric.WithDescription("Number of manifests we have received"))),
	validationTime: must(meter.Float64Histogram("f3_validation_time",
		metric.WithDescription("Histogram of time spent validating broadcasted in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0),
		metric.WithUnit("s"),
	)),
	proposalFetchTime: must(meter.Float64Histogram("f3_proposal_fetch_time",
		metric.WithDescription("Histogram of time spent fetching proposal per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0),
		metric.WithUnit("s"),
	)),
	committeeFetchTime: must(meter.Float64Histogram("f3_committee_fetch_time",
		metric.WithDescription("Histogram of time spent fetching committees per instance in seconds"),
		metric.WithExplicitBucketBoundaries(0.001, 0.003, 0.005, 0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0),
		metric.WithUnit("s"),
	)),
	validationDuplicateMessages:       must(meter.Int64Counter("f3_validation_duplicate_messages", metric.WithDescription("Number of duplicate GPBFT messages validated."))),
	validationDuplicateJustifications: must(meter.Int64Counter("f3_validation_duplicate_justifications", metric.WithDescription("Number of duplicate GPBFT justifications validated."))),
}

func recordValidationDuplicates(ctx context.Context, msg gpbft.ValidatedMessage) {
	// The given msg and its validated value should never be nil; but defensively
	// check anyway.
	if msg == nil || msg.Message() == nil {
		return
	}
	vmsg := msg.Message()
	if sampleSets.messages.contains(vmsg.Signature) {
		metrics.validationDuplicateMessages.Add(ctx, 1)
	}
	if vmsg.Justification != nil && sampleSets.justifications.contains(vmsg.Justification.Signature) {
		metrics.validationDuplicateJustifications.Add(ctx, 1)
	}
}

func recordValidationTime(ctx context.Context, start time.Time, result pubsub.ValidationResult) {
	var v string
	switch result {
	case pubsub.ValidationAccept:
		v = "accepted"
	case pubsub.ValidationReject:
		v = "rejected"
	case pubsub.ValidationIgnore:
		v = "ignored"
	default:
		v = "unknown"
	}
	metrics.validationTime.Record(
		ctx,
		time.Since(start).Seconds(),
		metric.WithAttributes(attribute.KeyValue{Key: "result", Value: attribute.StringValue(v)}))
}

// attrStatusFromErr returns an attribute with key "status" and value set to "success" if
// err is nil, and "failure" otherwise.
func attrStatusFromErr(err error) attribute.KeyValue {
	var v string
	switch err {
	case nil:
		v = "success"
	default:
		v = "failure"
	}
	return attribute.String("status", v)
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

// sampleSet stores a bounded set of samples and exposes the ability to check
// whether it contains a given sample. See sampleSet.contains.
//
// Internally, sampleSet uses two maps to store samples, each of which can grow
// up to the specified max size. When one map fills up, the sampleSet switches to
// the other, effectively flipping between them. This allows the set to check for
// sample existence across a range of approximately max size to twice the max size,
// offering a larger sample set compared to implementations that track insertion
// order with similar memory footprint.
//
// The worst case memory footprint of sampleSet is around 2 * maxSize * 96
// bytes.
type sampleSet struct {

	// We can use existing LRU implementations for this at the price of slightly
	// higher memory footprint and explanation that recency is unused. Hence the
	// hand-rolled data structure here.

	// maxSize defines the maximum number of samples to store per each internal set.
	maxSize int
	// mu protects access to flip and flop.
	mu sync.Mutex
	// flip stores one set of samples until until it reaches maxSize.
	flip map[string]struct{}
	// flop stores another set of samples until it reaches maxSize.
	flop map[string]struct{}
}

// newSampleSet creates a new sampleSet with a specified max size per sample
// subset.
func newSampleSet(maxSize int) *sampleSet {
	maxSize = max(1, maxSize)
	return &sampleSet{
		maxSize: maxSize,
		flip:    make(map[string]struct{}, maxSize),
		flop:    make(map[string]struct{}, maxSize),
	}
}

// contains checks if the given sample v is contained within flip set.
func (ss *sampleSet) contains(v []byte) bool {
	// The number 96 comes from the length of BLS signatures, the kind of value we
	// expect as the argument. Defensively re-slice it if it is larger at the price
	// of losing accuracy.
	//
	// Alternatively we could hash the values but considering the memory footprint of
	// these measurements (sub 10MB for a total of 50K samples) we choose larger
	// memory consumption over CPU footprint.
	key := string(v[:min(len(v), 96)])

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Check if the sample exists in either sets and if not insert it.
	if _, exists := ss.flip[key]; exists {
		return true
	}
	if _, exists := ss.flop[key]; exists {
		return true
	}
	ss.flip[key] = struct{}{}

	// Check if flip exceeds maxSize and if so do the flippity flop.
	if len(ss.flip) >= ss.maxSize {
		clear(ss.flop)
		ss.flop, ss.flip = ss.flip, ss.flop
	}
	return false
}
