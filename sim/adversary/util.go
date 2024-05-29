package adversary

import "github.com/filecoin-project/go-f3/gpbft"

type validatedMessage struct {
	msg *gpbft.GMessage
}

var _ gpbft.ValidatedMessage = (*validatedMessage)(nil)

func Validated(msg *gpbft.GMessage) gpbft.ValidatedMessage {
	return &validatedMessage{msg: msg}
}

func (v *validatedMessage) Message() *gpbft.GMessage {
	return v.msg
}
