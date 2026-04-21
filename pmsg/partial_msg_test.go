package pmsg

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/stretchr/testify/require"
)

func TestCompleteMessage(t *testing.T) {
	// chainex is unused by the cases below; cases requiring resolution are
	// exercised through the integration/emulator tests.
	pmm := &PartialMessageManager{}
	ctx := context.Background()

	commitForBottom := &gpbft.GMessage{
		Sender: 1,
		Vote: gpbft.Payload{
			Instance: 42,
			Phase:    gpbft.COMMIT_PHASE,
			Value:    &gpbft.ECChain{},
		},
	}

	for _, tc := range []struct {
		name          string
		pgmsg         *gpbft.PartialGMessage
		wantMsg       *gpbft.GMessage
		wantCompleted bool
	}{
		{
			name: "nil input",
		},
		{
			name:          "COMMIT for bottom",
			pgmsg:         &gpbft.PartialGMessage{GMessage: commitForBottom},
			wantMsg:       commitForBottom,
			wantCompleted: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotMsg, gotCompleted := pmm.CompleteMessage(ctx, tc.pgmsg)
			require.Equal(t, tc.wantCompleted, gotCompleted)
			require.Same(t, tc.wantMsg, gotMsg)
		})
	}
}

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
