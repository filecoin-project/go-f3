package f3

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/manifest"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/metric"
)

type gpbftInputs struct {
	manifest  *manifest.Manifest
	certStore *certstore.Store
	ec        ec.Backend
	verifier  gpbft.Verifier
	clock     clock.Clock

	ptCache *lru.Cache[string, cid.Cid]
}

func newInputs(manifest *manifest.Manifest, certStore *certstore.Store, ec ec.Backend,
	verifier gpbft.Verifier, clk clock.Clock) gpbftInputs {
	cache, err := lru.New[string, cid.Cid](256) // keep a bit more than 2x max ECChain size
	if err != nil {
		// panic as it only depends on the size
		panic(fmt.Errorf("could not create cache: %w", err))
	}

	return gpbftInputs{
		manifest:  manifest,
		certStore: certStore,
		ec:        ec,
		verifier:  verifier,
		clock:     clk,
		ptCache:   cache,
	}
}

func (h *gpbftInputs) getPowerTableCIDForTipset(ctx context.Context, tsk gpbft.TipSetKey) (cid.Cid, error) {
	sTSK := string(tsk)
	ptCid, ok := h.ptCache.Get(sTSK)
	if ok {
		return ptCid, nil
	}

	pt, err := h.ec.GetPowerTable(ctx, tsk)
	if err != nil {
		return cid.Undef, fmt.Errorf("getting power table to compute CID: %w", err)
	}
	ptCid, err = certs.MakePowerTableCID(pt)
	if err != nil {
		return cid.Undef, fmt.Errorf("computing power table CID: %w", err)
	}

	h.ptCache.Add(sTSK, ptCid)
	return ptCid, nil
}

func (h *gpbftInputs) collectChain(ctx context.Context, base ec.TipSet, head ec.TipSet) ([]ec.TipSet, error) {
	// TODO: optimize when head is way beyond base
	res := make([]ec.TipSet, 0, 2*gpbft.ChainMaxLen)
	res = append(res, head)

	current := head
	for !bytes.Equal(current.Key(), base.Key()) {
		if current.Epoch() < base.Epoch() {
			metrics.headDiverged.Add(ctx, 1)
			log.Infow("reorg-ed away from base, proposing just base",
				"head", head.String(), "base", base.String())
			return nil, nil
		}
		var err error
		current, err = h.ec.GetParent(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("walking back the chain: %w", err)
		}
		res = append(res, current)
	}
	slices.Reverse(res)
	return res[1:], nil
}

// Returns inputs to the next GPBFT instance.
// These are:
// - the supplemental data.
// - the EC chain to propose.
// These will be used as input to a subsequent instance of the protocol.
// The chain should be a suffix of the last chain notified to the host via
// ReceiveDecision (or known to be final via some other channel).
func (h *gpbftInputs) GetProposal(ctx context.Context, instance uint64) (_ *gpbft.SupplementalData, _ gpbft.ECChain, _err error) {
	defer func(start time.Time) {
		metrics.proposalFetchTime.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrStatusFromErr(_err)))
	}(time.Now())

	var baseTsk gpbft.TipSetKey
	if instance == h.manifest.InitialInstance {
		ts, err := h.ec.GetTipsetByEpoch(ctx,
			h.manifest.BootstrapEpoch-h.manifest.EC.Finality)
		if err != nil {
			return nil, nil, fmt.Errorf("getting boostrap base: %w", err)
		}
		baseTsk = ts.Key()
	} else {
		cert, err := h.certStore.Get(ctx, instance-1)
		if err != nil {
			return nil, nil, fmt.Errorf("getting cert for previous instance(%d): %w", instance-1, err)
		}
		baseTsk = cert.ECChain.Head().Key
	}

	baseTs, err := h.ec.GetTipset(ctx, baseTsk)
	if err != nil {
		return nil, nil, fmt.Errorf("getting base TS: %w", err)
	}
	headTs, err := h.ec.GetHead(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("getting head TS: %w", err)
	}

	collectedChain, err := h.collectChain(ctx, baseTs, headTs)
	if err != nil {
		return nil, nil, fmt.Errorf("collecting chain: %w", err)
	}

	// If we have an explicit head-lookback, trim the chain.
	if h.manifest.EC.HeadLookback > 0 {
		collectedChain = collectedChain[:max(0, len(collectedChain)-h.manifest.EC.HeadLookback)]
	}

	// less than ECPeriod since production of the head agreement is unlikely, trim the chain.
	if len(collectedChain) > 0 && h.clock.Since(collectedChain[len(collectedChain)-1].Timestamp()) < h.manifest.EC.Period {
		collectedChain = collectedChain[:len(collectedChain)-1]
	}

	base := gpbft.TipSet{
		Epoch: baseTs.Epoch(),
		Key:   baseTs.Key(),
	}
	base.PowerTable, err = h.getPowerTableCIDForTipset(ctx, baseTs.Key())
	if err != nil {
		return nil, nil, fmt.Errorf("computing powertable CID for base: %w", err)
	}

	suffixLen := min(gpbft.ChainMaxLen, h.manifest.Gpbft.ChainProposedLength) - 1 // -1 because of base
	suffix := make([]gpbft.TipSet, min(suffixLen, len(collectedChain)))
	for i := range suffix {
		suffix[i].Key = collectedChain[i].Key()
		suffix[i].Epoch = collectedChain[i].Epoch()

		suffix[i].PowerTable, err = h.getPowerTableCIDForTipset(ctx, suffix[i].Key)
		if err != nil {
			return nil, nil, fmt.Errorf("computing powertable CID for suffix %d: %w", i, err)
		}
	}
	chain, err := gpbft.NewChain(base, suffix...)
	if err != nil {
		return nil, nil, fmt.Errorf("making new chain: %w", err)
	}

	var supplData gpbft.SupplementalData
	committee, err := h.GetCommittee(ctx, instance+1)
	if err != nil {
		return nil, nil, fmt.Errorf("getting commite for %d: %w", instance+1, err)
	}

	supplData.PowerTable, err = certs.MakePowerTableCID(committee.PowerTable.Entries)
	if err != nil {
		return nil, nil, fmt.Errorf("making power table cid for supplemental data: %w", err)
	}

	return &supplData, chain, nil
}

func (h *gpbftInputs) GetCommittee(ctx context.Context, instance uint64) (_ *gpbft.Committee, _err error) {
	defer func(start time.Time) {
		metrics.committeeFetchTime.Record(context.TODO(), time.Since(start).Seconds(), metric.WithAttributes(attrStatusFromErr(_err)))
	}(time.Now())

	var powerTsk gpbft.TipSetKey
	var powerEntries gpbft.PowerEntries
	var err error

	if instance < h.manifest.InitialInstance+h.manifest.CommitteeLookback {
		//boostrap phase
		powerEntries, err = h.certStore.GetPowerTable(ctx, h.manifest.InitialInstance)
		if err != nil {
			return nil, fmt.Errorf("getting power table: %w", err)
		}
		if h.certStore.Latest() == nil {
			ts, err := h.ec.GetTipsetByEpoch(ctx, h.manifest.BootstrapEpoch-h.manifest.EC.Finality)
			if err != nil {
				return nil, fmt.Errorf("getting tipset for boostrap epoch with lookback: %w", err)
			}
			powerTsk = ts.Key()
		} else {
			cert, err := h.certStore.Get(ctx, h.manifest.InitialInstance)
			if err != nil {
				return nil, fmt.Errorf("getting finality certificate: %w", err)
			}
			powerTsk = cert.ECChain.Base().Key
		}
	} else {
		cert, err := h.certStore.Get(ctx, instance-h.manifest.CommitteeLookback)
		if err != nil {
			return nil, fmt.Errorf("getting finality certificate: %w", err)
		}
		powerTsk = cert.ECChain.Head().Key

		powerEntries, err = h.certStore.GetPowerTable(ctx, instance)
		if err != nil {
			log.Debugf("failed getting power table from certstore: %v, falling back to EC", err)

			powerEntries, err = h.ec.GetPowerTable(ctx, powerTsk)
			if err != nil {
				return nil, fmt.Errorf("getting power table: %w", err)
			}
		}
	}

	ts, err := h.ec.GetTipset(ctx, powerTsk)
	if err != nil {
		return nil, fmt.Errorf("getting tipset: %w", err)
	}

	table := gpbft.NewPowerTable()
	if err := table.Add(powerEntries...); err != nil {
		return nil, fmt.Errorf("adding entries to power table: %w", err)
	}
	if err := table.Validate(); err != nil {
		return nil, fmt.Errorf("invalid power table for instance %d: %w", instance, err)
	}

	// NOTE: we're intentionally keeping participants here even if they have no
	// effective power (after rounding power) to simplify things. The runtime cost is
	// minimal and it means that the keys can be aggregated before any rounding is done.
	// TODO: this is slow and under a lock, but we only want to do it once per
	// instance... ideally we'd have a per-instance lock/once, but that probably isn't
	// worth it.
	agg, err := h.verifier.Aggregate(table.Entries.PublicKeys())
	if err != nil {
		return nil, fmt.Errorf("failed to pre-compute aggregate mask for instance %d: %w", instance, err)
	}

	return &gpbft.Committee{
		PowerTable:        table,
		Beacon:            ts.Beacon(),
		AggregateVerifier: agg,
	}, nil
}
