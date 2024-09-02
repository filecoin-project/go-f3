package sim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMessageQueue_IsInAscendingOrderOfDeliverAt(t *testing.T) {
	subject := newMessagePriorityQueue()

	want1st := &messageInFlight{deliverAt: time.Time{}.Add(time.Second)}
	want2nd := &messageInFlight{deliverAt: want1st.deliverAt.Add(12 * time.Second)}
	want3rd := &messageInFlight{deliverAt: want2nd.deliverAt.Add(17 * time.Second)}
	want4th := &messageInFlight{deliverAt: want3rd.deliverAt.Add(100 * time.Second)}

	subject.Insert(want2nd)
	subject.Insert(want4th)
	subject.Insert(want1st)
	subject.Insert(want3rd)

	require.Equal(t, subject.Len(), 4)
	require.Equal(t, want1st, subject.Remove())
	require.Equal(t, want2nd, subject.Remove())
	require.Equal(t, want3rd, subject.Remove())
	require.Equal(t, want4th, subject.Remove())
	require.Zero(t, subject.Len())
}

func TestMessageQueue_ReturnsNilWhenEmpty(t *testing.T) {
	subject := newMessagePriorityQueue()
	wantMsg := &messageInFlight{
		source:    0,
		dest:      2,
		payload:   "fish",
		deliverAt: time.Time{}.Add(17 * time.Second),
	}

	subject.Insert(wantMsg)

	require.Equal(t, subject.Len(), 1)
	require.Equal(t, wantMsg, subject.Remove())
	require.Equal(t, subject.Len(), 0)
	require.Nil(t, subject.Remove())
}

func TestMessageQueue_ContainsAllDuplicateMessages(t *testing.T) {
	const insertions = 5
	subject := newMessagePriorityQueue()
	msg := &messageInFlight{deliverAt: time.Time{}.Add(17 * time.Second)}
	for i := 0; i < insertions; i++ {
		subject.Insert(msg)
	}
	require.Equal(t, subject.Len(), insertions)
	for i := 0; i < insertions; i++ {
		require.Equal(t, msg, subject.Remove())
	}
	require.Equal(t, subject.Len(), 0)
}

func TestMessageQueue_UpsertFirstWhere(t *testing.T) {
	// Sample values are ordered by their name, i.e. m1 is expected to be queued
	// before m2, m2 before m3 and so on. This is to make reading the test cases and
	// expected behaviour easier.
	var (
		m1 = &messageInFlight{
			source:    17,
			dest:      2,
			payload:   "fish",
			deliverAt: time.Time{}.Add(3 * time.Millisecond),
		}
		m2 = &messageInFlight{
			source:    6,
			dest:      7,
			payload:   "lobster",
			deliverAt: time.Time{}.Add(7 * time.Second),
		}
		m3 = &messageInFlight{
			source:    88,
			dest:      88,
			payload:   "barreleye",
			deliverAt: time.Time{}.Add(22 * time.Minute),
		}
		m4 = &messageInFlight{
			source:    8989,
			dest:      0,
			payload:   "fishmonger",
			deliverAt: time.Time{}.Add(63 * time.Hour),
		}
	)

	tests := []struct {
		name            string
		givenInsertions []*messageInFlight
		givenMatcher    func(*messageInFlight) bool
		givenUpsert     *messageInFlight
		wantMessages    []*messageInFlight
	}{
		{
			name:            "matching none inserts",
			givenInsertions: []*messageInFlight{m2, m4},
			givenMatcher:    func(*messageInFlight) bool { return false },
			givenUpsert:     m1,
			wantMessages:    []*messageInFlight{m1, m2, m4},
		},
		{
			name:            "matching one updates",
			givenInsertions: []*messageInFlight{m1, m2, m4},
			givenMatcher:    func(msg *messageInFlight) bool { return msg.payload == m1.payload },
			givenUpsert:     m3,
			wantMessages:    []*messageInFlight{m2, m3, m4},
		},
		{
			name:            "matching more than one updates first",
			givenInsertions: []*messageInFlight{m1, m2, m4},
			givenMatcher:    func(msg *messageInFlight) bool { return msg.payload == m2.payload || msg.payload == m4.payload },
			givenUpsert:     m3,
			wantMessages:    []*messageInFlight{m1, m3, m4},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject := newMessagePriorityQueue()
			for _, insertion := range test.givenInsertions {
				subject.Insert(insertion)
			}
			require.Equal(t, subject.Len(), len(test.givenInsertions))

			subject.UpsertFirstWhere(test.givenMatcher, test.givenUpsert)

			wantLen := len(test.wantMessages)
			require.Equal(t, wantLen, subject.Len())
			var previous *messageInFlight
			for i := 0; i < wantLen; i++ {
				wantMessage := test.wantMessages[i]
				next := subject.Remove()

				// Assert each field is equal except index, since it is modified to -1 prior to removal for safety.
				require.Equal(t, wantMessage.source, next.source)
				require.Equal(t, wantMessage.dest, next.dest)
				require.Equal(t, wantMessage.payload, next.payload)
				require.Equal(t, wantMessage.deliverAt, next.deliverAt)
				require.Equal(t, -1, next.index)

				// Sanity check that test.wantMessages are in order of their delivery at in case
				// of human error.
				if previous == nil {
					previous = next
				} else {
					require.True(t, previous.deliverAt.Before(next.deliverAt) || previous.deliverAt.Equal(next.deliverAt))
				}
			}
			require.Zero(t, subject.Len())
		})
	}
}
