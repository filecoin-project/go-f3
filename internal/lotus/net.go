package lotus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/go-f3/gpbft"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("lotus")

type resultOrError[R any] struct {
	Result R `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	}
}

func ListAllPeers(ctx context.Context, apiEndpoints ...string) []peer.AddrInfo {
	if len(apiEndpoints) == 0 {
		return nil
	}
	const netPeers = `{"method":"Filecoin.NetPeers","params":[],"id":2,"jsonrpc":"2.0"}`
	var addrs []peer.AddrInfo
	seen := make(map[string]struct{})
	for _, endpoint := range apiEndpoints {
		peers, err := doJsonRpcRequest[[]peer.AddrInfo](ctx, endpoint, netPeers)
		if err != nil {
			logger.Errorw("failed to get net peers from endpoint", "endpoint", endpoint, "err", err)
			continue
		}
		for _, addr := range peers {
			k := addr.ID.String()
			if _, found := seen[k]; !found {
				addrs = append(addrs, addr)
				seen[k] = struct{}{}
			}
		}
	}
	return addrs
}

func GetF3Progress(ctx context.Context, apiEndpoints ...string) []gpbft.InstanceProgress {
	if len(apiEndpoints) == 0 {
		return nil
	}
	const getF3Progress = `{"method":"Filecoin.F3GetProgress","params":[],"id":2,"jsonrpc":"2.0"}`
	progresses := make([]gpbft.InstanceProgress, 0, len(apiEndpoints))
	for _, endpoint := range apiEndpoints {
		progress, err := doJsonRpcRequest[gpbft.InstanceProgress](ctx, endpoint, getF3Progress)
		if err != nil {
			logger.Errorw("failed to get F3 progress from endpoint", "endpoint", endpoint, "err", err)
			continue
		}
		progresses = append(progresses, progress)
	}
	return progresses
}

func doJsonRpcRequest[R any](ctx context.Context, endpoint string, body string) (R, error) {
	var zeroResult R
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader([]byte(body)))
	if err != nil {
		return zeroResult, fmt.Errorf("failed to construct request: %w", err)
	}
	req.Header.Set("Content-Type", `application/json`)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return zeroResult, fmt.Errorf("failed to execute the request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return zeroResult, fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode != 200 {
		return zeroResult, fmt.Errorf("unsuccessful response status %d: %s", resp.StatusCode, string(respBody))
	}
	var roe resultOrError[R]
	if err := json.Unmarshal(respBody, &roe); err != nil {
		return zeroResult, fmt.Errorf("failed to unmarshal response as json: %s", string(respBody))
	}
	if roe.Error != nil {
		logger.Errorf("failed to discover peers from lotus daemon %s: %s", endpoint, roe.Error.Message)
		return zeroResult, fmt.Errorf("json rpc error: %s", roe.Error.Message)
	}
	return roe.Result, nil
}
