package lotus

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("lotus")

func ListAllPeers(ctx context.Context, apiEndpoints ...string) []peer.AddrInfo {
	if len(apiEndpoints) == 0 {
		return nil
	}

	const netPeersJsonRpc = `{"method":"Filecoin.NetPeers","params":[],"id":2,"jsonrpc":"2.0"}`
	type resultOrError struct {
		Result []peer.AddrInfo `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		}
	}
	var addrs []peer.AddrInfo
	seen := make(map[string]struct{})
	for _, endpoint := range apiEndpoints {
		body := bytes.NewReader([]byte(netPeersJsonRpc))
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, body)
		if err != nil {
			logger.Errorf("failed construct request to discover peers from: %s :%w", endpoint, err)
			continue
		}
		req.Header.Set("Content-Type", `application/json`)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			logger.Errorf("failed to discover peers from lotus daemon %s: %w", endpoint, err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Errorf("failed to read response body from lotus daemon %s: %w", endpoint, err)
			continue
		}
		var roe resultOrError
		if err := json.Unmarshal(respBody, &roe); err != nil {
			logger.Errorf("failed to unmarshal response from lotus daemon %s: %s: %w", endpoint, string(respBody), err)
			continue
		}
		if roe.Error != nil {
			logger.Errorf("failed to discover peers from lotus daemon %s: %s", endpoint, roe.Error.Message)
			continue
		}

		for _, addr := range roe.Result {
			k := addr.ID.String()
			if _, found := seen[k]; !found {
				addrs = append(addrs, addr)
				seen[k] = struct{}{}
			}
		}
	}
	return addrs
}
