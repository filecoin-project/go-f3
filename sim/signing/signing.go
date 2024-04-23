package signing

import "github.com/filecoin-project/go-f3/gpbft"

type Backend interface {
	gpbft.Signer
	gpbft.Verifier
	GenerateKey() (gpbft.PubKey, any)
}
