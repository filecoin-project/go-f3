package main

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/f3"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./f3/gen.go", "f3",
		f3.GMessage{},
		f3.Payload{},
		f3.Justification{},
		f3.TipSet{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
