package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/observer"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/urfave/cli/v2"
)

var observerCmd = cli.Command{
	Name:  "observe",
	Usage: "observes and logs messages in F3 network",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:  "identity",
			Usage: "The path to protobuf encoded libp2p identity of the observer.",
			Value: "./observer/identity",
		},
		&cli.StringFlag{
			Name:    "networkName",
			Aliases: []string{"nn"},
			Usage:   "The network name.",
		},
		&cli.StringSliceFlag{
			Name:  "bootstrapAddr",
			Usage: "The list of bootstrap addrs.",
		},
		&cli.PathFlag{
			Name: "bootstrapAddrsFile",
			Usage: "The list of bootstrap addrs read from a file with one address per line. " +
				"The entries are used in conjunction with any addresses specified via <bootstrapAddr>.",
		},
		&cli.IntFlag{
			Name:  "subBufferSize",
			Usage: "The buffer size for the pubsub subscription channel.",
			Value: 1024,
		},
		&cli.StringFlag{
			Name:  "queryServerListenAddr",
			Usage: "The query server listen address.",
			Value: ":42080",
		},
		&cli.StringFlag{
			Name:  "rotatePath",
			Usage: "The query server listen address.",
			Value: ".",
		},
		&cli.DurationFlag{
			Name:  "rotateInterval",
			Usage: "The observed messages rotation interval.",
			Value: 1 * time.Hour,
		},
		&cli.DurationFlag{
			Name:  "retention",
			Usage: "The maximum length of time to keep the rotated files.",
			Value: 2 * 7 * 24 * time.Hour,
		},
		&cli.StringFlag{
			Name:        "dataSourceName",
			Usage:       "The observer database DSN",
			DefaultText: "In memory",
			Value:       "",
		},
		&cli.IntFlag{
			Name:  "connLo",
			Usage: "The lower connection manager watermark.",
			Value: 160,
		},
		&cli.IntFlag{
			Name:  "connHi",
			Usage: "The higher connection manager watermark.",
			Value: 192,
		},
	},

	Action: func(cctx *cli.Context) error {
		opts := []observer.Option{
			observer.WithQueryServerListenAddress(cctx.String("queryServerListenAddr")),
			observer.WithSubscriptionBufferSize(cctx.Int("subBufferSize")),
			observer.WithRotatePath(cctx.String("rotatePath")),
			observer.WithRotateInterval(cctx.Duration("rotateInterval")),
			observer.WithRetention(cctx.Duration("retention")),
			observer.WithDataSourceName(cctx.String("dataSourceName")),
		}
		var identity crypto.PrivKey
		if cctx.IsSet("identity") {
			marshaledKey, err := os.ReadFile(cctx.String("identity"))
			if err != nil {
				return fmt.Errorf("failed to read libp2p identity: %w", err)
			}
			identity, err = crypto.UnmarshalPrivateKey(marshaledKey)
			if err != nil {
				return fmt.Errorf("failed to decode libp2p identity: %w", err)
			}
		} else {
			var err error
			identity, _, err = crypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				return fmt.Errorf("failed to generate libp2p identity: %w", err)
			}
		}

		if cctx.IsSet("networkName") {
			opts = append(opts, observer.WithStaticNetworkName(gpbft.NetworkName(cctx.String("networkName"))))
		}
		if cctx.IsSet("bootstrapAddr") {
			opts = append(opts, observer.WithBootstrapAddrsFromString(cctx.StringSlice("bootstrapAddr")...))
		}
		if cctx.IsSet("bootstrapAddrsFile") {
			baf, err := os.ReadFile(cctx.Path("bootstrapAddrsFile"))
			if err != nil {
				return fmt.Errorf("failed to read bootstrap addrs file: %w", err)
			}
			bootstrapAddrs := strings.Split(strings.TrimSpace(string(baf)), "\n")
			opts = append(opts, observer.WithBootstrapAddrsFromString(bootstrapAddrs...))
		}
		if observerID, err := peer.IDFromPrivateKey(identity); err != nil {
			return fmt.Errorf("failed to get peer ID from libp2p identity: %w", err)
		} else {
			_, _ = fmt.Fprintf(cctx.App.Writer, "Observer peer ID: %s\n", observerID)
		}

		connMngr, err := connmgr.NewConnManager(cctx.Int("connLo"), cctx.Int("connHi"))
		if err != nil {
			return err
		}
		host, err := libp2p.New(
			libp2p.Identity(identity),
			libp2p.UserAgent("f3-observer"),
			libp2p.ConnectionManager(connMngr),
		)
		if err != nil {
			return fmt.Errorf("failed to create libp2p host: %w", err)
		}
		opts = append(opts, observer.WithHost(host))

		o, err := observer.New(opts...)
		if err != nil {
			return fmt.Errorf("failed to instantiate observer: %w", err)
		}
		if err := o.Start(cctx.Context); err != nil {
			return fmt.Errorf("failed to start observer: %w", err)
		}

		<-cctx.Context.Done()
		_, _ = fmt.Fprintf(cctx.App.Writer, "Stopping observer\n")
		return o.Stop(context.Background())
	},
}
