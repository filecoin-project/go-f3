package sim

import (
	"container/heap"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

var _ heap.Interface = (*messageQueue)(nil)

// messageQueue is a priority queue that implements heap.Interface and holds
// simulation messages in flight prioritised by their delivery time in ascending
// order.
type messageQueue struct {
	mailbox []*messageInFlight
}

type messageInFlight struct {
	source    gpbft.ActorID // ID of the sender
	dest      gpbft.ActorID // ID of the receiver
	payload   any           // Message body
	deliverAt time.Time     // Timestamp at which to deliver the message

	index int // Index in the heap used internally by the heap implementation
}

func (m *messageInFlight) isAlarm() bool {
	switch payload := m.payload.(type) {
	case string:
		return m.dest == m.source && payload == "ALARM"
	default:
		return false
	}
}

func newMessagePriorityQueue() *messageQueue {
	var mpq messageQueue
	heap.Init(&mpq)
	return &mpq
}

// Len returns the number of messages that are currently in-flight.
func (pq *messageQueue) Len() int { return len(pq.mailbox) }

// Less determines whether message in-flight at index i should be prioritised
// before the message in-flight at index j. The message deliverAt filed is used
// to determine this, where the earlier the deliverAt the higher priority the
// message, i.e. messages are sorted in ascending order of their deliverAt.
//
// Note that alarms take precedence over messages when both have the same
// deliverAt.
//
// This function is part of heap.Interface and must not be called externally.
func (pq *messageQueue) Less(i, j int) bool {
	// We want Pop to give us the earliest delivery time, so we use Less to sort by
	// deliverAt in ascending order.
	switch one, other := pq.mailbox[i], pq.mailbox[j]; {
	case one.deliverAt.Equal(other.deliverAt):
		return one.isAlarm() && !other.isAlarm()
	default:
		return one.deliverAt.Before(other.deliverAt)
	}
}

// Swap swaps messages in-flight at index i with the one at index j.
//
// This function is part of heap.Interface and must not be called externally.
func (pq *messageQueue) Swap(i, j int) {
	pq.mailbox[i], pq.mailbox[j] = pq.mailbox[j], pq.mailbox[i]
	pq.mailbox[i].index = i
	pq.mailbox[j].index = j
}

// Push adds an element to this queue.
//
// This function is part of heap.Interface and must not be called externally.
// See: Insert.
func (pq *messageQueue) Push(x any) {
	n := len(pq.mailbox)
	item := x.(*messageInFlight)
	item.index = n
	pq.mailbox = append(pq.mailbox, item)
}

// Pop removes and returns the Len() - 1 element.
//
// This function is part of heap.Interface and must not be called externally.
// See: Remove.
func (pq *messageQueue) Pop() any {
	old := pq.mailbox
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.mailbox = old[0 : n-1]
	return item
}

// Insert adds a messageInFlight to the queue.
func (pq *messageQueue) Insert(x *messageInFlight) {
	heap.Push(pq, x)
}

// Remove removes and returns the earliest messageInFlight from the queue.
func (pq *messageQueue) Remove() *messageInFlight {
	if pq.Len() > 0 {
		return heap.Pop(pq).(*messageInFlight)
	}
	return nil
}

// UpsertFirstWhere finds the first message that matches the given criteria, and
// if found updates its content to the upsert message. Otherwise, inserts the
// message to the queue.
func (pq *messageQueue) UpsertFirstWhere(match func(*messageInFlight) bool, upsert *messageInFlight) {
	for _, msg := range pq.mailbox {
		if match(msg) {
			msg.source = upsert.source
			msg.dest = upsert.dest
			msg.deliverAt = upsert.deliverAt
			msg.payload = upsert.payload
			heap.Fix(pq, msg.index)
			return
		}
	}
	pq.Insert(upsert)
}
