package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/urfave/cli/v2"
)

var toolsCmd = cli.Command{
	Name:  "tools",
	Usage: "various tools for f3",
	Subcommands: []*cli.Command{
		&ptCidCmd,
	},
}

var ptCidCmd = cli.Command{
	Name:  "ptCid",
	Usage: "compute the CID of a json power table",
	Action: func(c *cli.Context) error {
		var entries gpbft.PowerEntries
		err := json.NewDecoder(os.Stdin).Decode(&entries)
		if err != nil {
			return fmt.Errorf("error while decoding: %w", err)
		}

		cid, err := certs.MakePowerTableCID(entries)
		if err != nil {
			return fmt.Errorf("error while computing CID: %w", err)
		}

		fmt.Printf("%s\n", cid)
		return nil
	},
}
