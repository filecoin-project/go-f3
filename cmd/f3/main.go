package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "f3",
		Usage: "standalone f3 node",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "manifest",
				Value: "manifest.json",
				Usage: "path to the manifest file",
			},
		},
		Commands: []*cli.Command{
			&runCmd,
			&manifestCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("runtime error: +%v\n", err)
		os.Exit(1)
	}
}
