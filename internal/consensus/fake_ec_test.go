package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTipsetString(t *testing.T) {
	ec := &FakeEC{}
	ts := ec.genTipset(1)
	require.True(t, len(ts.String()) != 0)
	t.Log(ts.String())
}
