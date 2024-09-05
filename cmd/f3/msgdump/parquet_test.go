package msgdump

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

var sampleMessageJSON = `{"UnixMicroTime":1723047990453259,"NetworkName":"testnetnet/1","Message":{"Sender":22352,"Vote":{"Instance":9,"Round":0,"Phase":5,"SupplementalData":{"Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk="},"Value":[{"Epoch":4158041,"Key":"AXGg5AIgsGmu9dWNsCG9YBE67100gPoU8eCoqGwsGRLxTqy70fYBcaDkAiA7TOrxgatUIdYancl6cIiFdMgtPJqjvP/5Z6yRP4PrVwFxoOQCIDKEai58tsmOy0cdqGmip3xIxKWxbqIgKLzQIXeHOyW1AXGg5AIgu6nRqVZI9JjjplGZNrfVh992Dy77PwfDVdgBKzT63qUBcaDkAiCR2nxb4FyBs4BbypT0ygV/LUMuUX5JfEa43Rvs9C3tCAFxoOQCIEZZhUO0pTh1R3F9A1x4OmDWi4+L04AYKWsUCuZ8SKrU","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},{"Epoch":4158042,"Key":"AXGg5AIgcaua1NcW/0t7O5Yr+5mkeLu9Xj7iwwnFAOiRCpiANvsBcaDkAiAlQlpraJB33Ttm9fj0xh3+hMXLRRsxffPnfEgt4HB9GA==","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},{"Epoch":4158043,"Key":"AXGg5AIgO4WgDY44KF+3S/g8Ksj+BTZiWAXsXsLs/ltUUirCPXoBcaDkAiB+e1I6r/vrCl//RP32mDExEAzHLiQbofO0g7DuBQ+S+wFxoOQCILFWQDpPl8dWxDr0Cu9RRpfY3gLFeQUJaRgFYZs0YC2MAXGg5AIgOIETEvAAle5EpWsj83n6hlJdmCqzVRY62v6e7vEHQfYBcaDkAiCNsHNANtqtVy+rFDjXmtBasidiN4sNZdQ6sjtGl+OWxg==","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}]},"Signature":"igXCqi8jKSuYse+Fgg1ue8dqQ2fs8C73LBxjyyg0od99dpOsbs07mMjPLSJ/1hqoGUhzQdfsWgtmz/D96W++pY8BsidnchUzBQd7MoqgDmncFg6jbgxRwYplp+NyVsjb","Ticket":null,"Justification":{"Vote":{"Instance":9,"Round":0,"Phase":4,"SupplementalData":{"Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk="},"Value":[{"Epoch":4158041,"Key":"AXGg5AIgsGmu9dWNsCG9YBE67100gPoU8eCoqGwsGRLxTqy70fYBcaDkAiA7TOrxgatUIdYancl6cIiFdMgtPJqjvP/5Z6yRP4PrVwFxoOQCIDKEai58tsmOy0cdqGmip3xIxKWxbqIgKLzQIXeHOyW1AXGg5AIgu6nRqVZI9JjjplGZNrfVh992Dy77PwfDVdgBKzT63qUBcaDkAiCR2nxb4FyBs4BbypT0ygV/LUMuUX5JfEa43Rvs9C3tCAFxoOQCIEZZhUO0pTh1R3F9A1x4OmDWi4+L04AYKWsUCuZ8SKrU","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},{"Epoch":4158042,"Key":"AXGg5AIgcaua1NcW/0t7O5Yr+5mkeLu9Xj7iwwnFAOiRCpiANvsBcaDkAiAlQlpraJB33Ttm9fj0xh3+hMXLRRsxffPnfEgt4HB9GA==","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},{"Epoch":4158043,"Key":"AXGg5AIgO4WgDY44KF+3S/g8Ksj+BTZiWAXsXsLs/ltUUirCPXoBcaDkAiB+e1I6r/vrCl//RP32mDExEAzHLiQbofO0g7DuBQ+S+wFxoOQCILFWQDpPl8dWxDr0Cu9RRpfY3gLFeQUJaRgFYZs0YC2MAXGg5AIgOIETEvAAle5EpWsj83n6hlJdmCqzVRY62v6e7vEHQfYBcaDkAiCNsHNANtqtVy+rFDjXmtBasidiN4sNZdQ6sjtGl+OWxg==","PowerTable":"AXGg5AIg0NYh/YFdOj1N45I1zYmUDyoUs9OpZX1zOQPXzBCeRVk=","Commitments":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}]},"Signers":[0,1,2,3],"Signature":"jlfS7Oairfycsh7cCkrmqOFE5+kl7WQOEFKmIcnTLW1y430B2txugrC/FNhi/cbPDuJ32ZSMDoFCjFwZpK1zjDPvIchy5dX3nGWGlNxMGSiRdhEtz47ANqZ7WuFoupTG"}}}`

func TestOpenAndWriteParquetFile(t *testing.T) {
	t.SkipNow()
	pw, err := NewParquetWriter[ParquetEnvelope]("/tmp/pq-test")
	require.NoError(t, err)

	var gEnvelope GMessageEnvelope
	require.NoError(t, json.Unmarshal([]byte(sampleMessageJSON), &gEnvelope))
	row, err := ToParquet(gEnvelope)
	require.NoError(t, err)
	_, err = pw.Write(row)
	require.NoError(t, err)
	_, err = pw.Write(row)
	require.NoError(t, err)
	require.NoError(t, pw.Close())

}
