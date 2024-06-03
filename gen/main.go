package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
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
		gpbft.PowerEntry{},
		gpbft.PowerEntries{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = gen.WriteTupleEncodersToFile("../certs/gen.go", "certs",
		certs.PowerTableDelta{},
		certs.FinalityCertificate{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = gen.WriteTupleEncodersToFile("../certexchange/gen.go", "certexchange",
		certexchange.Request{},
		certexchange.ResponseHeader{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
