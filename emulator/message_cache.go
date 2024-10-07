package emulator

import "github.com/filecoin-project/go-f3/gpbft"

// MessageCache is a repository of messages keyed by their instance, round and
// phase. This cache is used for testing purposes only and has no eviction
// strategy. It is primarily used to store messages from self for rebroadcast.
type MessageCache map[gpbft.Instant]*gpbft.GMessage

func NewMessageCache() MessageCache {
	return make(map[gpbft.Instant]*gpbft.GMessage)
}

func (mc MessageCache) Get(instant gpbft.Instant) (*gpbft.GMessage, bool) {
	msg, found := mc[instant]
	return msg, found
}

func (mc MessageCache) PutIfAbsent(msg *gpbft.GMessage) bool {
	key := gpbft.Instant{
		ID:    msg.Vote.Instance,
		Round: msg.Vote.Round,
		Phase: msg.Vote.Phase,
	}
	if _, found := mc[key]; found {
		return false
	}
	mc[key] = msg
	return true
}
