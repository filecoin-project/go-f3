package manifest

import (
	"context"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter   = otel.Meter("f3/manifest")
	metrics = struct {
		senderManifestPublished             metric.Int64Counter
		senderManifestUpdated               metric.Int64Counter
		senderManifestInfoPaused            metric.Int64Gauge
		senderManifestInfoProtocolVersion   metric.Int64Gauge
		senderManifestInfoInitialInstance   metric.Int64Gauge
		senderManifestInfoBootstrapEpoch    metric.Int64Gauge
		senderManifestInfoCommitteeLookback metric.Int64Gauge
		senderManifestInfoSequenceNumber    metric.Int64Gauge
		senderInfo                          metric.Int64Gauge
	}{
		senderManifestPublished: measurements.Must(meter.Int64Counter("f3_manifest_sender_published",
			metric.WithDescription("Number of times manifest sender has published a manifest."))),
		senderManifestUpdated: measurements.Must(meter.Int64Counter("f3_manifest_sender_updated",
			metric.WithDescription("Number of times the manifest known by the sender has been updated."))),
		senderManifestInfoPaused: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_paused",
			metric.WithDescription("Sender's latest manifest value for Paused."))),
		senderManifestInfoProtocolVersion: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_protocol_version",
			metric.WithDescription("Sender's latest manifest value for ProtocolVersion."))),
		senderManifestInfoInitialInstance: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_initial_instance",
			metric.WithDescription("Sender's latest manifest value for InitialInstance."))),
		senderManifestInfoBootstrapEpoch: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_bootstrap_epoch",
			metric.WithDescription("Sender's latest manifest value for BootstrapEpoch."))),
		senderManifestInfoCommitteeLookback: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_committee_lookback",
			metric.WithDescription("Sender's latest manifest value for CommitteeLookback."))),
		senderManifestInfoSequenceNumber: measurements.Must(meter.Int64Gauge("f3_manifest_sender_manifest_info_seq_num",
			metric.WithDescription("Sender's latest manifest value for Sequence Number."))),
		senderInfo: measurements.Must(meter.Int64Gauge("f3_manifest_sender_info",
			metric.WithDescription("The metadata about the manifest sender."))),
	}

	attrStatusSuccess = attribute.String("status", "success")
	attrStatusFailure = attribute.String("status", "failure")
)

func recordSenderPublishManifest(ctx context.Context, err error) {
	if err != nil {
		metrics.senderManifestPublished.Add(ctx, 1, metric.WithAttributes(attrStatusFailure))
	} else {
		metrics.senderManifestPublished.Add(ctx, 1, metric.WithAttributes(attrStatusSuccess))
	}
}

func recordSenderManifestInfo(ctx context.Context, seq uint64, manifest *Manifest) {
	// Only record manifest info if manifest is not nil. Also see:
	// https://github.com/filecoin-project/go-f3/issues/502
	if manifest == nil {
		return
	}

	attrs := metric.WithAttributes(attribute.String("network", string(manifest.NetworkName)))
	if manifest.Pause {
		metrics.senderManifestInfoPaused.Record(ctx, 1, attrs)
	} else {
		metrics.senderManifestInfoPaused.Record(ctx, 0, attrs)
	}
	metrics.senderManifestInfoProtocolVersion.Record(ctx, int64(manifest.ProtocolVersion), attrs)
	metrics.senderManifestInfoInitialInstance.Record(ctx, int64(manifest.InitialInstance), attrs)
	metrics.senderManifestInfoBootstrapEpoch.Record(ctx, manifest.BootstrapEpoch, attrs)
	metrics.senderManifestInfoCommitteeLookback.Record(ctx, int64(manifest.CommitteeLookback), attrs)
	metrics.senderManifestInfoSequenceNumber.Record(ctx, int64(seq), attrs)
}
