package certexchange

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/gpbft"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

var log = logging.Logger("f3/certexchange")

const maxResponseLen = 256

// Server is libp2p a certificate exchange server.
type Server struct {
	// Request timeouts. If non-zero, requests will be canceled after the specified duration.
	RequestTimeout time.Duration
	NetworkName    gpbft.NetworkName
	Host           host.Host
	Store          *certstore.Store

	// - held (read) by all active requests.
	// - taken (write) on shutdown to block until said requests complete.
	runningLk sync.RWMutex
	stopFunc  context.CancelFunc
}

func (s *Server) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.RequestTimeout > 0 {
		return context.WithTimeout(ctx, s.RequestTimeout)
	}
	return ctx, func() {}
}

func (s *Server) handleRequest(ctx context.Context, stream network.Stream) (_err error) {
	serverMetrics.requests.Add(ctx, 1)
	start := time.Now()
	defer func() {
		if perr := recover(); perr != nil {
			_err = fmt.Errorf("panicked in server response: %v", perr)
			log.Errorf("%s\n%s", _err, string(debug.Stack()))
		}
		if _err != nil {
			serverMetrics.requestsFailed.Add(ctx, 1)
			serverMetrics.responseTimeMS.Record(ctx, time.Since(start).Milliseconds())
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		// Not all transports support deadlines.
		_ = stream.SetDeadline(deadline)
	}

	br := bufio.NewReader(stream)
	bw := bufio.NewWriter(stream)

	// Request has no variable-length fields, so we don't need a limited reader.
	var req Request
	if err := req.UnmarshalCBOR(br); err != nil {
		log.Debugf("failed to read request from stream: %v", err)
		return err
	}

	limit := req.Limit
	if limit > maxResponseLen {
		limit = maxResponseLen
	}
	var resp ResponseHeader
	if latest := s.Store.Latest(); latest != nil {
		resp.PendingInstance = latest.GPBFTInstance + 1
	}

	if resp.PendingInstance >= req.FirstInstance && req.IncludePowerTable {
		serverMetrics.powerTablesServed.Add(ctx, 1)
		pt, err := s.Store.GetPowerTable(ctx, req.FirstInstance)
		if err != nil {
			log.Errorf("failed to load power table: %v", err)
			return err
		}
		resp.PowerTable = pt
	}

	if err := resp.MarshalCBOR(bw); err != nil {
		log.Debugf("failed to write header to stream: %v", err)
		return err
	}

	if resp.PendingInstance > req.FirstInstance {
		// Only try to return up-to but not including the pending instance we just told the
		// client about. Otherwise we could return instances _beyond_ that which is
		// inconsistent and confusing.
		end := req.FirstInstance + limit
		if end >= resp.PendingInstance {
			end = resp.PendingInstance - 1
		}

		certs, err := s.Store.GetRange(ctx, req.FirstInstance, end)
		if err == nil || errors.Is(err, certstore.ErrCertNotFound) {
			serverMetrics.certificatesServed.Add(ctx, int64(len(certs)))
			serverMetrics.certificatesServedPerResponse.Record(ctx, int64(len(certs)))
			for i := range certs {
				if err := certs[i].MarshalCBOR(bw); err != nil {
					log.Debugf("failed to write certificate to stream: %v", err)
					return err
				}
			}
		} else {
			log.Errorf("failed to load finality certificates: %v", err)
		}
	} else {
		serverMetrics.certificatesServedPerResponse.Record(ctx, 0)
	}
	return bw.Flush()
}

// Start the server.
func (s *Server) Start(startCtx context.Context) error {
	s.runningLk.Lock()
	defer s.runningLk.Unlock()
	if s.stopFunc != nil {
		return fmt.Errorf("certificate exchange already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.stopFunc = cancel
	s.Host.SetStreamHandler(FetchProtocolName(s.NetworkName), func(stream network.Stream) {
		// Hold the read-lock for the duration of the request so shutdown can block on
		// closing all request handlers.
		if !s.runningLk.TryRLock() {
			// We use a try-lock because blocking means we're trying to shutdown.
			_ = stream.Reset()
			return
		}

		defer s.runningLk.RUnlock()

		// Short-circuit if we're already closed.
		if ctx.Err() != nil {
			_ = stream.Reset()
			return
		}

		// Kill the stream if/when we shutdown the server.
		defer context.AfterFunc(ctx, func() { _ = stream.Reset() })()

		ctx, cancel := s.withDeadline(ctx)
		defer cancel()

		if err := s.handleRequest(ctx, stream); err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.Close()
		}

	})
	return nil
}

// Stop the server.
func (s *Server) Stop(stopCtx context.Context) error {
	// Ask the handlers to cancel/stop.
	s.runningLk.RLock()
	if s.stopFunc != nil {
		s.stopFunc()
	}
	s.runningLk.RUnlock()

	// Take the write-lock to wait for all outstanding requests to return.
	s.runningLk.Lock()
	defer s.runningLk.Unlock()
	if s.stopFunc == nil {
		return nil
	}
	s.stopFunc = nil
	s.Host.RemoveStreamHandler(FetchProtocolName(s.NetworkName))

	return nil
}
