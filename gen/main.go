package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/gpbft"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./gpbft/gen.go", "gpbft",
		gpbft.GMessage{},
		gpbft.Payload{},
		gpbft.Justification{},
		gpbft.TipSet{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
