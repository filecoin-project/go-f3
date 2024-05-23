package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)

	go func() {
		<-sigChan
		cancel()
	}()

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %+v\n", err)
		os.Exit(1)
	}
}
