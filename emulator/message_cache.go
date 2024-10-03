package emulator

import "github.com/filecoin-project/go-f3/gpbft"

type MessageKey struct {
	Instance, Round uint64
	Phase           gpbft.Phase
}

// MessageCache is a repository of messages keyed by their instance, round and
// phase. This cache is used for testing purposes only and has no eviction
// strategy. It is primarily used to store messages from self for rebroadcast.
type MessageCache map[MessageKey]*gpbft.GMessage

func NewMessageCache() MessageCache {
	return make(map[MessageKey]*gpbft.GMessage)
}

func (mc MessageCache) Get(instance, round uint64, phase gpbft.Phase) (*gpbft.GMessage, bool) {
	msg, found := mc[MessageKey{
		Instance: instance,
		Round:    round,
		Phase:    phase,
	}]
	return msg, found
}

func (mc MessageCache) PutIfAbsent(msg *gpbft.GMessage) bool {
	key := MessageKey{
		Instance: msg.Vote.Instance,
		Round:    msg.Vote.Round,
		Phase:    msg.Vote.Phase,
	}
	if _, found := mc[key]; found {
		return false
	}
	mc[key] = msg
	return true
}
