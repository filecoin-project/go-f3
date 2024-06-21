package signing

import "github.com/filecoin-project/go-f3/gpbft"

type Backend interface {
	gpbft.Signer
	gpbft.Verifier
	gpbft.SigningMarshaler
	GenerateKey() (gpbft.PubKey, any)
}
