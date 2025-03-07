package f3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_roundDownToUnixTime(t *testing.T) {
	someTime, err := time.Parse(time.RFC3339Nano, "2024-03-07T15:06:20.522847852Z")
	require.NoError(t, err)

	for _, test := range []struct {
		name     string
		at       time.Time
		interval time.Duration
		want     int64
	}{
		{
			name:     "millisecond",
			at:       someTime,
			interval: time.Millisecond * 200,
			want:     1709823980400, // 2024-03-07 15:06:20.4
		},
		{
			name:     "second",
			at:       someTime,
			interval: time.Second * 7,
			want:     1709823976000, // 2024-03-07 15:06:16
		},
		{
			name:     "bigBang",
			at:       time.Unix(0, 0),
			interval: time.Second * 7,
			want:     0,
		},
		{
			name:     "justAfterBigBang",
			at:       time.Unix(5, 0),
			interval: time.Second * 5,
			want:     5000,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := roundDownToUnixMilliTime(test.at, test.interval)
			require.Equal(t, test.want, got)
			require.GreaterOrEqual(t, got, time.Microsecond.Milliseconds())
			require.LessOrEqual(t, got, test.at.UnixMilli())
		})
	}
}
