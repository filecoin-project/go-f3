package writeaheadlog

import (
	"github.com/filecoin-project/go-f3/gpbft"
)

type MessageWriteAheadLog struct {
	delegate *WriteAheadLog[MessageEntry, *MessageEntry]
}

func OpenMessageWriteAheadLog(path string) (*MessageWriteAheadLog, error) {
	delegate, err := Open[MessageEntry, *MessageEntry](path)
	if err != nil {
		return nil, err
	}
	return &MessageWriteAheadLog{
		delegate: delegate,
	}, nil
}

func (mwal *MessageWriteAheadLog) ForEach(apply func(message *gpbft.GMessage) bool) {
	mwal.delegate.ForEach(
		func(entry MessageEntry) bool {
			return apply(entry.Message)
		},
	)
}

func (mwal *MessageWriteAheadLog) Append(msg *gpbft.GMessage) error {
	return mwal.delegate.Append(MessageEntry{Message: msg})
}

func (mwal *MessageWriteAheadLog) Purge(upToEpoch uint64) error {
	return mwal.delegate.Purge(upToEpoch)
}

func (mwal *MessageWriteAheadLog) FindMessages(instance, round uint64, phase gpbft.Phase) []*gpbft.GMessage {
	var matches []*gpbft.GMessage
	mwal.ForEach(
		func(subject *gpbft.GMessage) bool {
			if subject.Vote.Instance == instance && subject.Vote.Round == round && subject.Vote.Phase == phase {
				matches = append(matches, subject)
			}
			return true
		},
	)
	return matches
}

func (mwal *MessageWriteAheadLog) Close() error {
	return mwal.delegate.Close()
}
