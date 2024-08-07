package measurements_test

import (
	"errors"
	"testing"

	"github.com/filecoin-project/go-f3/internal/measurements"
	"github.com/stretchr/testify/require"
)

func TestUtils_Must(t *testing.T) {
	require.Panics(t, func() {
		measurements.Must("fish", errors.New("🐠"))
	})
	require.Equal(t, "fish", measurements.Must("fish", nil))
}
