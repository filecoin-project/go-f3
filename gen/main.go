package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/gpbft"
	gen "github.com/whyrusleeping/cbor-gen"
)

//go:generate go run .

func main() {
	err := gen.WriteTupleEncodersToFile("../gpbft/gen.go", "gpbft",
		gpbft.TipSet{},
		gpbft.GMessage{},
		gpbft.SupplementalData{},
		gpbft.Payload{},
		gpbft.Justification{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
