package powerstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/manifest"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("f3/ohshitstore")

var _ ec.Backend = (*Store)(nil)

type Store struct {
	ec.Backend

	ds       datastore.Datastore
	cs       *certstore.Store
	manifest *manifest.Manifest

	clock      clock.Clock
	runningCtx context.Context
	cancel     context.CancelFunc
	errgrp     *errgroup.Group

	lastStoredPt    gpbft.PowerEntries
	lastStoredEpoch int64
}

const diffPrefix = "/powerdiffs"

func New(ctx context.Context, ec ec.Backend, ds datastore.Datastore, cs *certstore.Store, manifest *manifest.Manifest) (*Store, error) {
	runningCtx, ctxCancel := context.WithCancel(context.WithoutCancel(ctx))
	errgrp, runningCtx := errgroup.WithContext(runningCtx)
	return &Store{
		Backend: ec,

		ds:         namespace.Wrap(ds, datastore.NewKey("/ohshitstore")),
		cs:         cs,
		manifest:   manifest,
		clock:      clock.GetClock(ctx),
		runningCtx: runningCtx,
		cancel:     ctxCancel,
		errgrp:     errgrp,
	}, nil
}

func (ps *Store) Start(ctx context.Context) error {
	ps.errgrp.Go(func() (_err error) {
		defer func() {
			if err := recover(); err != nil {
				_err = fmt.Errorf("PANIC in power store: %+v", err)
				log.Errorw("PANIC in power store", "error", _err)
			} else if _err != nil && ps.runningCtx.Err() == nil {
				log.Errorw("power store exited early", "error", _err)
			}
		}()
		return ps.run(ps.runningCtx)
	})
	return nil
}

func (ps *Store) Stop(ctx context.Context) error {
	ps.cancel()
	return ps.errgrp.Wait()
}

func (ps *Store) GetPowerTable(ctx context.Context, tsk gpbft.TipSetKey) (gpbft.PowerEntries, error) {
	pt, ptErr := ps.Backend.GetPowerTable(ctx, tsk)
	if ptErr == nil {
		return pt, nil
	}

	ts, err := ps.GetTipset(ctx, tsk)
	if err != nil {
		return nil, fmt.Errorf("failed to load tipset with key %s: %w", tsk, err)
	}

	targetEpoch := ts.Epoch()
	baseEpoch, basePt, err := ps.basePowerTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load base power table: %w", err)
	}

	// We might fail here if we receive a decision from the network while trying to participate
	// in an instance. In practice, we should be robust against this method failing _anyways_
	// (e.g., if we're really behind or something).
	//
	// TODO: Consider searching backwards for a better base?
	if targetEpoch < baseEpoch {
		return nil, fmt.Errorf("target epoch %d before the latest F3 finalized base %d, move on already", targetEpoch, baseEpoch)
	} else if targetEpoch == baseEpoch {
		return basePt, nil
	}

	diffs := make([]certs.PowerTableDiff, 0, targetEpoch-baseEpoch)
	for baseEpoch < targetEpoch {
		baseEpoch++
		diff, err := ps.get(ctx, baseEpoch)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, diff)
	}
	return certs.ApplyPowerTableDiffs(basePt, diffs...)
}

func (ps *Store) f3PowerBase(ctx context.Context) (int64, uint64, error) {
	baseEpoch := ps.manifest.BootstrapEpoch - ps.manifest.ECFinality
	baseInstance := ps.manifest.InitialInstance
	if lastCert := ps.cs.Latest(); lastCert != nil {
		baseInstance = lastCert.GPBFTInstance + 1
		if lastCert.GPBFTInstance > ps.manifest.InitialInstance+ps.manifest.CommitteeLookback {
			baseCert, err := ps.cs.Get(ctx, lastCert.GPBFTInstance-ps.manifest.CommitteeLookback)
			if err != nil {
				return 0, 0, err
			}
			baseEpoch = baseCert.ECChain.Head().Epoch
		}
	}
	return baseEpoch, baseInstance, nil
}

func (ps *Store) basePowerTable(ctx context.Context) (int64, gpbft.PowerEntries, error) {
	baseEpoch, baseInstance, err := ps.f3PowerBase(ctx)
	if err != nil {
		return 0, nil, err
	}

	basePt, err := ps.cs.GetPowerTable(ctx, baseInstance)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to load base power table: %w", err)
	}
	return baseEpoch, basePt, nil
}

func (ps *Store) mostRecentPowerTable(ctx context.Context) (int64, gpbft.PowerEntries, error) {
	baseEpoch, basePt, err := ps.basePowerTable(ctx)
	if err != nil {
		return 0, nil, err
	}

	var diffs []certs.PowerTableDiff
	for {
		diff, err := ps.get(ctx, baseEpoch+1)
		if err != nil {
			if !errors.Is(err, datastore.ErrNotFound) {
				log.Errorw("failed to lookup power table delta", "epoch", baseEpoch+1, "error", err)
			}
			break
		}
		diffs = append(diffs, diff)
		baseEpoch++
	}
	basePt, err = certs.ApplyPowerTableDiffs(basePt, diffs...)
	if err != nil {
		return 0, nil, err
	}
	return baseEpoch, basePt, nil
}

func (ps *Store) put(ctx context.Context, epoch int64, diff certs.PowerTableDiff) error {
	var buf bytes.Buffer
	if err := diff.MarshalCBOR(&buf); err != nil {
		return err
	}
	if err := ps.ds.Put(ctx, ps.dsKeyForDiff(epoch), buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ps *Store) deleteAll(ctx context.Context) {
	res, err := ps.ds.Query(ctx, query.Query{Prefix: diffPrefix})
	if err != nil {
		log.Errorw("failed to query for power-table diffs to delete", "error", err)
		return
	}
	defer res.Close()
	for r, ok := res.NextSync(); ok; r, ok = res.NextSync() {
		if r.Error != nil {
			log.Errorw("failed to query for power-table diffs to delete", "error", r.Error)
			continue
		}

		if err := ps.ds.Delete(ctx, datastore.NewKey(r.Key)); err != nil {
			log.Errorw("failed to delete power-table diffs to delete", "error", err)
			return
		}
	}
}

func (ps *Store) run(ctx context.Context) error {
	ticker := ps.clock.Ticker(2 * ps.manifest.ECPeriod)
	defer ticker.Stop()

	startThreshold := max(ps.manifest.ECFinality*3/2, 1)
	stopThreshold := max(ps.manifest.ECFinality/2, 1)

	var initialized bool
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}

		f3Base, _, err := ps.f3PowerBase(ctx)
		if err != nil {
			log.Errorw("failed to determine f3 base epoch", "error", err)
		}
		ecHeadTs, err := ps.GetHead(ctx)
		if err != nil {
			log.Errorw("failed to get EC head", "error", err)
			continue
		}
		ecHead := ecHeadTs.Epoch()

		log := log.With(zap.Int64("ecHead", ecHead), zap.Int64("f3Base", f3Base))

		if !initialized && f3Base > ecHead-stopThreshold {
			initialized = true
			log.Debugw("Clearing the OhShitStore on initialization because we're caught-up.")
			ps.deleteAll(ctx)
			ps.lastStoredEpoch = -1
			ps.lastStoredPt = nil
			continue
		}
		initialized = true

		if ps.lastStoredPt == nil {
			if f3Base > ecHead-startThreshold {
				log.Debugw("skipping catch-up because we're within the start threshold")
				continue
			}
			log.Warnf("Uh-oh, F3 has fallen behind EC by more than %d epochs! Engaging the OhShitStore™.", startThreshold)
			ps.lastStoredEpoch, ps.lastStoredPt, err = ps.mostRecentPowerTable(ctx)
			if err != nil {
				log.Errorw("failed to lookup most recent power table", "error", err)
				continue
			}
		} else if f3Base > ecHead-stopThreshold {
			log.Infow("Stopping the OhShitStore™ because we're caught-up")
			ps.deleteAll(ctx)
			ps.lastStoredEpoch = -1
			ps.lastStoredPt = nil
			continue
		}
		if err := ps.advance(ctx, ecHead); err != nil {
			log.Errorw("failed to store power tables", "last-stored", ps.lastStoredEpoch, "error", err)
		}
	}
	return nil
}

func (ps *Store) advance(ctx context.Context, head int64) error {
	targetEpoch := head - ps.manifest.ECFinality
	// Only store tipsets before finality.
	if ps.lastStoredEpoch >= targetEpoch {
		return nil
	}

	ts, err := ps.GetTipsetByEpoch(ctx, targetEpoch)
	if err != nil {
		return err
	}

	// Due to null epochs, we may get a tipset from before the target. If we do, record the null
	// epochs and move on.
	if ts.Epoch() <= ps.lastStoredEpoch {
		return ps.fillNullEpochs(ctx, targetEpoch)
	}

	// Load the tipsets in reverse order, then reverse. GetParent is O(1),
	// GetTipsetByEpoch is O(N) where N is how far back in history we are.
	chain := make([]ec.TipSet, 0, ts.Epoch()-ps.lastStoredEpoch)
	chain = append(chain, ts)
	for ts.Epoch() > ps.lastStoredEpoch+1 {
		ts, err = ps.GetParent(ctx, ts)
		if err != nil {
			return err
		}
		// We might have skipped an epoch due to null tipsets.
		if ts.Epoch() <= ps.lastStoredEpoch {
			break
		}
		chain = append(chain, ts)
	}
	slices.Reverse(chain)

	for _, ts := range chain {
		// Fill in null epochs.
		if err := ps.fillNullEpochs(ctx, ts.Epoch()-1); err != nil {
			return err
		}

		pt, err := ps.Backend.GetPowerTable(ctx, ts.Key())
		if err != nil {
			return err
		}
		diff := certs.MakePowerTableDiff(ps.lastStoredPt, pt)
		if err := ps.put(ctx, ps.lastStoredEpoch+1, diff); err != nil {
			return err
		}
		ps.lastStoredEpoch++
		ps.lastStoredPt = pt
	}

	return ps.fillNullEpochs(ctx, targetEpoch)
}

func (ps *Store) fillNullEpochs(ctx context.Context, until int64) error {
	for ; ps.lastStoredEpoch < until; ps.lastStoredEpoch++ {
		if err := ps.put(ctx, ps.lastStoredEpoch+1, nil); err != nil {
			return fmt.Errorf("failed to record power delta for null tipset at epoch %d", ps.lastStoredEpoch+1)
		}
	}
	return nil
}

func (ps *Store) get(ctx context.Context, epoch int64) (certs.PowerTableDiff, error) {
	if epoch < 0 {
		return nil, fmt.Errorf("negative epoch (%d) cannot have a power table", epoch)
	}
	buf, err := ps.ds.Get(ctx, ps.dsKeyForDiff(epoch))
	if err != nil {
		return nil, err
	}
	var diff certs.PowerTableDiff
	if err := diff.UnmarshalCBOR(bytes.NewReader(buf)); err != nil {
		return nil, err
	}

	return diff, nil
}

func (ps *Store) dsKeyForDiff(epoch int64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("%s/%016X", diffPrefix, epoch))
}
