package sim

import (
	"sort"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

type messageInFlight struct {
	source    gpbft.ActorID // ID of the sender
	dest      gpbft.ActorID // ID of the receiver
	payload   interface{}   // Message body
	deliverAt time.Time     // Timestamp at which to deliver the message
}

// A queue of directed messages, maintained as an ordered list.
type messageQueue []messageInFlight

func (h *messageQueue) Insert(x messageInFlight) {
	// Insert the new message after any messages with a sooner or equal deliverAt.
	i := sort.Search(len(*h), func(i int) bool {
		ith := (*h)[i].deliverAt
		return ith.After(x.deliverAt)
	})
	*h = append(*h, messageInFlight{})
	copy((*h)[i+1:], (*h)[i:])
	(*h)[i] = x
}

// Removes an entry from the queue
func (h *messageQueue) Remove(i int) messageInFlight {
	v := (*h)[i]
	copy((*h)[i:], (*h)[i+1:])
	*h = (*h)[:len(*h)-1]
	return v
}

// Removes all entries from the queue that satisfy a predicate.
func (h *messageQueue) RemoveWhere(f func(messageInFlight) bool) {
	i := 0
	for _, x := range *h {
		if !f(x) {
			(*h)[i] = x
			i++
		}
	}
	*h = (*h)[:i]
}
