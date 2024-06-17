package main

import (
	"encoding/json"
	"math/big"
	"os"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/signing"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var manifestCmd = cli.Command{
	Name: "manifest",
	Subcommands: []*cli.Command{
		&manifestGenCmd,
	},
}
var manifestGenCmd = cli.Command{
	Name:  "gen",
	Usage: "generates f3 manifest",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "N",
			Usage: "number of participant",
			Value: 2,
		},
	},

	Action: func(c *cli.Context) error {
		path := c.String("manifest")
		m := f3.LocalnetManifest()

		fsig := signing.NewFakeBackend()
		for i := 0; i < c.Int("N"); i++ {
			pubkey, _ := fsig.GenerateKey()

			m.InitialPowerTable = append(m.InitialPowerTable, gpbft.PowerEntry{
				ID:     gpbft.ActorID(i),
				PubKey: pubkey,
				Power:  big.NewInt(1000),
			})
		}

		f, err := os.OpenFile(path, os.O_WRONLY, 0666)
		if err != nil {
			return xerrors.Errorf("opening manifest file for writing: %w", err)
		}
		err = json.NewEncoder(f).Encode(m)
		if err != nil {
			return xerrors.Errorf("encoding manifest: %w", err)
		}
		err = f.Close()
		if err != nil {
			return xerrors.Errorf("closing file: %w", err)
		}

		return nil
	},
}

func getManifest(c *cli.Context) (f3.Manifest, error) {
	manifestPath := c.String("manifest")
	return loadManifest(manifestPath)
}

func loadManifest(path string) (f3.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return f3.Manifest{}, xerrors.Errorf("opening %s to load manifest: %w", path, err)
	}
	defer f.Close()
	var m f3.Manifest

	err = json.NewDecoder(f).Decode(&m)
	if err != nil {
		return f3.Manifest{}, xerrors.Errorf("decoding JSON: %w", err)
	}
	return m, err
}
