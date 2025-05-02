//go:generate go run golang.org/x/tools/cmd/stringer@v0.32.0 -type=PollStatus
package polling

import (
	"context"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A Poller will poll specific peers on-demand to try to advance the current GPBFT instance.
type Poller struct {
	*certexchange.Client

	Store             *certstore.Store
	SignatureVerifier gpbft.Verifier
	PowerTable        gpbft.PowerEntries
	NextInstance      uint64
	clock             clock.Clock
}

// NewPoller constructs a new certificate poller and initializes it from the passed certificate store.
func NewPoller(ctx context.Context, client *certexchange.Client, store *certstore.Store, verifier gpbft.Verifier) (*Poller, error) {
	var nextInstance uint64
	if latest := store.Latest(); latest != nil {
		nextInstance = latest.GPBFTInstance + 1
	}
	pt, err := store.GetPowerTable(ctx, nextInstance)
	if err != nil {
		return nil, err
	}
	return &Poller{
		Client:            client,
		Store:             store,
		SignatureVerifier: verifier,
		NextInstance:      nextInstance,
		PowerTable:        pt,
		clock:             clock.GetClock(ctx),
	}, nil
}

type PollResult struct {
	Status  PollStatus
	Latency time.Duration
	Error   error

	// NewCertificates certificates we didn't yet have. Excludes certificates we received through some other
	// channel while polling.
	NewCertificates uint64
	// Total certificates received.
	ReceivedCertificates uint64
}

type PollStatus int

const (
	PollMiss PollStatus = iota
	PollHit
	PollFailed
	PollIllegal
)

func (p PollStatus) GoString() string {
	return p.String()
}

// CatchUp attempts to advance to the latest instance from the certificate store without making any
// network requests. It returns the number of instances we advanced.
func (p *Poller) CatchUp(ctx context.Context) (uint64, error) {
	latest := p.Store.Latest()
	if latest == nil {
		return 0, nil
	}

	next := latest.GPBFTInstance + 1
	progress := next - p.NextInstance

	if progress == 0 {
		return 0, nil
	}

	pt, err := p.Store.GetPowerTable(ctx, next)
	if err != nil {
		return 0, err
	}
	p.PowerTable = pt
	p.NextInstance = next
	return progress, nil
}

// Poll polls a specific peer, possibly multiple times, in order to advance the instance as much as
// possible. It returns:
//
// 1. A PollResult indicating the outcome: miss, hit, failed, illegal.
// 2. An error if something went wrong internally (e.g., the certificate store returned an error).
func (p *Poller) Poll(ctx context.Context, peer peer.ID) (*PollResult, error) {
	res := new(PollResult)

	// Cancel this context on exit in case we exit early before the request finishes.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		// Requests take time, so always try to catch-up between requests in case there has
		// been some "local" action from the GPBFT instance.
		if _, err := p.CatchUp(ctx); err != nil {
			return nil, err
		}

		start := p.clock.Now()
		resp, ch, err := p.Request(ctx, peer, &certexchange.Request{
			FirstInstance:     p.NextInstance,
			Limit:             maxRequestLength,
			IncludePowerTable: false,
		})
		res.Latency = p.clock.Since(start)
		if err != nil {
			res.Status = PollFailed
			res.Error = err
			return res, nil
		}

		// If they're caught up, record it as a hit. Otherwise, if they have nothing
		// to give us, move on.
		if resp.PendingInstance >= p.NextInstance {
			res.Status = PollHit
		}

		for cert := range ch {
			// TODO: consider batching verification, it's slightly faster.
			next, _, pt, err := certs.ValidateFinalityCertificates(
				p.SignatureVerifier, p.NetworkName, p.PowerTable, p.NextInstance, nil,
				cert,
			)
			if err != nil {
				res.Status = PollIllegal
				res.Error = err
				return res, nil
			}
			res.ReceivedCertificates++

			// We check if we've already received this certificate not as an
			// optimization but to determine whether or not this request was actually
			// useful. This check is inherently racy; even if we made the check/put
			// atomic, we'd still race with GPBFT finishing the current instance.
			if l := p.Store.Latest(); l == nil || cert.GPBFTInstance > l.GPBFTInstance {
				if err := p.Store.Put(ctx, cert); err != nil {
					return nil, err
				}
				res.NewCertificates++
			}
			p.NextInstance = next
			p.PowerTable = pt
		}

		// Try again if they're claiming to have more instances (and gave me at
		// least one).
		if resp.PendingInstance <= p.NextInstance {
			return res, nil
		} else if res.ReceivedCertificates == 0 {
			res.Status = PollFailed
			// If they give me no certificates but claim to have more, treat this as a
			// failure (could be a connection failure, etc).
			return res, nil
		}

	}
}
