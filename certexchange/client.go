package certexchange

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
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

	Log f3.Logger
}

func resetOnCancel(ctx context.Context, s network.Stream) func() {
	errCh := make(chan error, 1)
	cancel := context.AfterFunc(ctx, func() {
		errCh <- s.Reset()
		close(errCh)
	})
	return func() {
		if cancel() {
			_ = s.Reset()
		} else {
			<-errCh
		}
	}
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
			c.Log.Error(_err)
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
	// canceled by the `cancel` function returned by withDeadline above.
	_ = resetOnCancel(ctx, stream)

	if deadline, ok := ctx.Deadline(); ok {
		if err := stream.SetDeadline(deadline); err != nil {
			return nil, nil, err
		}
	}

	br := &io.LimitedReader{R: bufio.NewReader(stream), N: 100}
	bw := bufio.NewWriter(stream)

	if err := req.MarshalCBOR(bw); err != nil {
		c.Log.Debugf("failed to marshal certificate exchange request to peer %s: %w", p, err)
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
		c.Log.Debugf("failed to unmarshal certificate exchange response header from peer %s: %w", p, err)
		return nil, nil, err
	}

	// Copy/replace the cancel func so exiting the request doesn't cancel it.
	cancelReq := cancel
	cancel = nil

	ch := make(chan *certs.FinalityCertificate, 1)
	// copy this in case the caller decides to re-use the request object...
	request := *req
	go func() {
		defer func() {
			if perr := recover(); perr != nil {
				c.Log.Errorf("panicked while receiving certificates from peer %s: %v\n%s", p, perr, string(debug.Stack()))
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
			if err != nil {
				c.Log.Debugf("failed to unmarshal certificate from peer %s: %w", p, err)
				return
			}
			// One quick sanity check. The rest will be validated by the caller.
			if cert.GPBFTInstance != request.FirstInstance+i {
				c.Log.Warnf("received out-of-order certificate from peer %s", p)
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
