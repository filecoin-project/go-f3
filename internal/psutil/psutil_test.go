package psutil_test

import (
	"testing"

	"github.com/filecoin-project/go-f3/internal/psutil"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"
)

func TestManifestMessageIdFn(t *testing.T) {
	for _, test := range []struct {
		name          string
		one           *pubsub_pb.Message
		other         *pubsub_pb.Message
		expectEqualID bool
	}{
		{
			name: "same topic different data",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("barreleye"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("barreleye"),
				Data:  []byte("lobstermuncher"),
			},
			expectEqualID: false,
		},
		{
			name: "same data different topic",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("barreleye"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("lobster"),
				From:  []byte("barreleye"),
				Data:  []byte("undadasea"),
			},
			expectEqualID: false,
		},
		{
			name: "same data and topic different sender",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("barreleye"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("fisherman"),
				Data:  []byte("undadasea"),
			},
			expectEqualID: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			this, that := psutil.ManifestMessageIdFn(test.one), psutil.ManifestMessageIdFn(test.other)
			require.Equal(t, test.expectEqualID, this == that)
		})
	}
}

func TestGPBFTMessageIdFn(t *testing.T) {
	for _, test := range []struct {
		name          string
		one           *pubsub_pb.Message
		other         *pubsub_pb.Message
		expectEqualID bool
	}{
		{
			name: "same topic different data",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("fish"),
				Data:  []byte("lobstermuncher"),
			},
			expectEqualID: false,
		},
		{
			name: "same data different topic",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("lobster"),
				Data:  []byte("undadasea"),
			},
			expectEqualID: false,
		},
		{
			name: "same data and topic different sender",
			one: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("barreleye"),
				Data:  []byte("undadasea"),
			},
			other: &pubsub_pb.Message{
				Topic: topic("fish"),
				From:  []byte("fisherman"),
				Data:  []byte("undadasea"),
			},
			expectEqualID: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			this, that := psutil.GPBFTMessageIdFn(test.one), psutil.GPBFTMessageIdFn(test.other)
			require.Equal(t, test.expectEqualID, this == that)
		})
	}
}

func topic(s string) *string { return &s }
