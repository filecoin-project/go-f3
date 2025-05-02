package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/filecoin-project/go-f3/manifest"
	"github.com/urfave/cli/v2"
)

var manifestCmd = cli.Command{
	Name: "manifest",
	Subcommands: []*cli.Command{
		&manifestGenCmd,
		&manifestCheckCmd,
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
		m := manifest.LocalDevnetManifest()

		if err := m.Validate(); err != nil {
			return fmt.Errorf("generated invalid manifest: %w", err)
		}

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("opening manifest file for writing: %w", err)
		}
		err = json.NewEncoder(f).Encode(m)
		if err != nil {
			return fmt.Errorf("encoding manifest: %w", err)
		}
		err = f.Close()
		if err != nil {
			return fmt.Errorf("closing file: %w", err)
		}

		return nil
	},
}

var manifestCheckCmd = cli.Command{
	Name:  "check",
	Usage: "validates an f3 manifest",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:     "manifest",
			Usage:    "The path to the manifest file.",
			Required: true,
		},
	},

	Action: func(c *cli.Context) error {
		path := c.String("manifest")
		currentManifest, err := loadManifest(path)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.App.Writer, "✅ valid manifest for %s\n", currentManifest.NetworkName)
		return nil
	},
}

func getManifest(c *cli.Context) (manifest.Manifest, error) {
	manifestPath := c.String("manifest")
	return loadManifest(manifestPath)
}

func loadManifest(path string) (manifest.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("opening %s to load manifest: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	unmarshalled, err := manifest.Unmarshal(f)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("unmarshalling %s to manifest: %w", path, err)
	}
	if unmarshalled == nil {
		return manifest.Manifest{}, fmt.Errorf("unmarshalling %s to manifest: manifest must be specified", path)
	}
	return *unmarshalled, nil
}
