package certexchange

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// We've estimated the max power table size to be less than 1MiB:
//
// 1. For 10k participants.
// 2. <100 bytes per entry (key + id + power)
const maxPowerTableSize = 1024 * 1024

// Client is a libp2p certificate exchange client for requesting finality certificates from specific
// peers.
type Client struct {
	Host           host.Host
	NetworkName    gpbft.NetworkName
	RequestTimeout time.Duration
}

func (c *Client) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.RequestTimeout > 0 {
		return context.WithTimeout(ctx, c.RequestTimeout)
	}
	return context.WithCancel(ctx)
}

// Request finality certificates from the specified peer. Returned finality certificates start at
// the requested instance number and are sequential, but are otherwise unvalidated.
func (c *Client) Request(ctx context.Context, p peer.ID, req *Request) (_rh *ResponseHeader, _ch <-chan *certs.FinalityCertificate, _err error) {
	defer func() {
		if perr := recover(); perr != nil {
			_err = fmt.Errorf("panicked requesting certificates from peer %s: %v\n%s", p, perr, string(debug.Stack()))
			log.Error(_err)
		}
	}()

	ctx, cancel := c.withDeadline(ctx)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	proto := FetchProtocolName(c.NetworkName)
	stream, err := c.Host.NewStream(ctx, p, proto)
	if err != nil {
		return nil, nil, err
	}
	// Reset the stream if the parent context is canceled. We never call the returned stop
	// function because we call the cancel function returned by `withDeadline` (which cancels
	// the entire context tree).
	unbindReset := context.AfterFunc(ctx, func() { _ = stream.Reset() })

	if deadline, ok := ctx.Deadline(); ok {
		// Not all transports support deadlines.
		_ = stream.SetDeadline(deadline)
	}

	br := &io.LimitedReader{R: bufio.NewReader(stream), N: 100}
	bw := bufio.NewWriter(stream)

	if err := req.MarshalCBOR(bw); err != nil {
		log.Debugf("failed to marshal certificate exchange request to peer %s: %w", p, err)
		return nil, nil, err
	}
	if err := bw.Flush(); err != nil {
		return nil, nil, err
	}
	if err := stream.CloseWrite(); err != nil {
		return nil, nil, err
	}

	var resp ResponseHeader
	if req.IncludePowerTable {
		br.N = maxPowerTableSize
	}
	err = resp.UnmarshalCBOR(br)
	if err != nil {
		log.Debugf("failed to unmarshal certificate exchange response header from peer %s: %w", p, err)
		return nil, nil, err
	}

	// If we aren't expecting any certificates, return immediately. We may _only_ want the power
	// table.
	if req.Limit == 0 {
		// Reset immediately instead of waiting for it to get run async (better cleanup
		// behavior).
		if unbindReset() {
			_ = stream.Reset()
		}

		ch := make(chan *certs.FinalityCertificate)
		close(ch)
		return &resp, ch, nil
	}

	ch := make(chan *certs.FinalityCertificate, 1)
	// copy this in case the caller decides to re-use the request object...
	request := *req

	// Copy/replace the cancel func so exiting the request doesn't cancel it.
	cancelReq := cancel
	cancel = nil
	go func() {
		defer func() {
			if perr := recover(); perr != nil {
				log.Errorf("panicked while receiving certificates from peer %s: %v\n%s", p, perr, string(debug.Stack()))
			}

			// Reset immediately instead of waiting for it to get run async (better cleanup
			// behavior). Also, because I don't fully trust AfterFunc.
			if unbindReset() {
				_ = stream.Reset()
			}

			cancelReq()
			close(ch)
		}()
		for i := uint64(0); i < request.Limit; i++ {
			cert := new(certs.FinalityCertificate)

			// We'll read at most 1MiB per certificate. They generally shouldn't be that
			// large, but large power deltas could get close.
			br.N = maxPowerTableSize
			err := cert.UnmarshalCBOR(br)
			switch err {
			case nil:
			case io.EOF:
				return
			default:
				log.Debugf("failed to unmarshal certificate from peer %s: %w", p, err)
				return
			}
			// One quick sanity check. The rest will be validated by the caller.
			if cert.GPBFTInstance != request.FirstInstance+i {
				log.Warnf("received out-of-order certificate from peer %s", p)
				return
			}

			select {
			case <-ctx.Done():
				return
			case ch <- cert:
			}
		}
	}()
	return &resp, ch, nil
}

func FindInitialPowerTable(ctx context.Context, c Client, powerTableCID cid.Cid, ecPeriod time.Duration) (gpbft.PowerEntries, error) {
	request := Request{
		FirstInstance:     0,
		Limit:             0,
		IncludePowerTable: true,
	}

	clk := clock.GetClock(ctx)
	ticker := clk.Ticker(ecPeriod / 2)
	defer ticker.Stop()

	pollOne := func(ctx context.Context, p peer.ID) (gpbft.PowerEntries, bool) {
		targetProtocol := FetchProtocolName(c.NetworkName)
		if proto, err := c.Host.Peerstore().FirstSupportedProtocol(p, targetProtocol); err != nil ||
			proto != targetProtocol {

			return nil, false
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		rh, _, err := c.Request(ctx, p, &request)
		if err != nil {
			log.Infow("requesting initial power table", "error", err, "peer", p)
			return nil, false
		}
		ptCID, err := certs.MakePowerTableCID(rh.PowerTable)
		if err != nil {
			log.Infow("computing iniital power table CID", "error", err)
			return nil, false
		}
		if !bytes.Equal(ptCID, powerTableCID.Bytes()) {
			log.Infow("peer returned missmatching power table", "peer", p)
			return nil, false
		}
		return rh.PowerTable, true
	}

	for {
		for _, p := range c.Host.Network().Peers() {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			if res, found := pollOne(ctx, p); found {
				return res, nil
			}
		}

		log.Infof("could not find anyone with initial power table, retrying after sleep")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
