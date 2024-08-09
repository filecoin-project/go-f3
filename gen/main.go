package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/cmd/f3/msgdump"
	"github.com/filecoin-project/go-f3/gpbft"
	gen "github.com/whyrusleeping/cbor-gen"
)

//go:generate go run .

func main() {
	err := gen.WriteTupleEncodersToFile("../gpbft/cbor_gen.go", "gpbft",
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

	err = gen.WriteTupleEncodersToFile("../certs/cbor_gen.go", "certs",
		certs.PowerTableDelta{},
		certs.PowerTableDiff{},
		certs.FinalityCertificate{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = gen.WriteTupleEncodersToFile("../certexchange/cbor_gen.go", "certexchange",
		certexchange.Request{},
		certexchange.ResponseHeader{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = gen.WriteTupleEncodersToFile("../cmd/f3/msgdump/cbor_gen.go", "msgdump",
		msgdump.GMessageEnvelope{},
		msgdump.GMessageEnvelopeDeffered{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
