package main

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var manifestCmd = cli.Command{
	Name: "manifest",
	Subcommands: []*cli.Command{
		&manifestGenCmd,
		&manifestServeCmd,
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

var manifestServeCmd = cli.Command{
	Name:  "serve",
	Usage: "serves f3 dynamic manifest server by periodically checking a given manifest file for change",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:  "identity",
			Usage: "The path to protobuf encoded libp2p identity of the server.",
		},
		&cli.PathFlag{
			Name:     "manifest",
			Usage:    "The path to the manifest file to monitor for change and serve.",
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:  "listenAddr",
			Usage: "The libp2p listen addrs.",
			Value: cli.NewStringSlice(
				"/ip4/0.0.0.0/tcp/45001",
				"/ip4/0.0.0.0/udp/45001/quic-v1",
				"/ip4/0.0.0.0/udp/45001/quic-v1/webtransport",
				"/ip6/::/tcp/45001",
				"/ip6/::/udp/45001/quic-v1",
				"/ip6/::/udp/45001/quic-v1/webtransport",
			),
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
		&cli.DurationFlag{
			Name:  "checkInterval",
			Usage: "The interval at which to check the manifest file for change.",
			Value: 5 * time.Second,
		},
		&cli.DurationFlag{
			Name:  "publishInterval",
			Usage: "The interval at which manifest is published on pubsub.",
			Value: 2 * pubsub.TimeCacheDuration,
		},
	},

	Action: func(c *cli.Context) error {
		var id crypto.PrivKey
		if c.IsSet("identity") {
			enId, err := os.ReadFile(c.String("identity"))
			if err != nil {
				return fmt.Errorf("reading libp2p identity file: %w", err)
			}
			id, err = crypto.UnmarshalPrivateKey(enId)
			if err != nil {
				return fmt.Errorf("unmarshalling libp2p identity: %w", err)
			}
		} else {
			_, _ = fmt.Fprintln(c.App.Writer, "No lbp2p identity set; using random identity.")
			var err error
			id, _, err = crypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				return fmt.Errorf("generating random libp2p identity: %w", err)
			}
		}

		peerID, err := peer.IDFromPrivateKey(id)
		if err != nil {
			return fmt.Errorf("getting peer ID from libp2p identity: %w", err)
		}
		_, _ = fmt.Fprintf(c.App.Writer, "Manifest server peer ID: %s\n", peerID)

		host, err := libp2p.New(
			libp2p.Identity(id),
			libp2p.ListenAddrStrings(c.StringSlice("listenAddr")...),
			libp2p.UserAgent("f3-dynamic-manifest-server"),
		)
		if err != nil {
			return fmt.Errorf("initializing libp2p host: %w", err)
		}

		defer func() { _ = host.Close() }()

		connectToBootstrappers := func() {
			bootstrappers := c.StringSlice("bootstrapAddr")
			if c.IsSet("bootstrapAddrsFile") {
				bootstrapersFile, err := os.Open(c.Path("bootstrapAddrsFile"))
				if err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to open bootstrapAddrsFile: %v\n", err)
					return
				}
				defer func() {
					_ = bootstrapersFile.Close()
				}()
				scanner := bufio.NewScanner(bootstrapersFile)
				for scanner.Scan() {
					bootstrappers = append(bootstrappers, scanner.Text())
				}
				if err := scanner.Err(); err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to read bootstrapAddrsFile: %v\n", err)
					return
				}
			}
			for _, bootstrapper := range bootstrappers {
				if maddr, err := multiaddr.NewMultiaddr(bootstrapper); err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to parse to bootstrap address: %s %v\n", bootstrapper, err)
				} else if multiaddrs, err := madns.Resolve(c.Context, maddr); err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to resolve bootstrap address: %s %v\n", bootstrapper, err)
				} else if paddrinfos, err := peer.AddrInfosFromP2pAddrs(multiaddrs...); err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to build peer.AddrInfo for bootstrap address: %s %v\n", bootstrapper, err)
				} else {
					for _, paddrinfo := range paddrinfos {
						if err := host.Connect(c.Context, paddrinfo); err != nil {
							_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to connect to bootstrap address: %v\n", err)
						}
					}
				}
			}
		}

		// Connect to bootstrappers once as soon as we start.
		connectToBootstrappers()

		manifestPath := c.String("manifest")
		currentManifest, err := loadManifest(manifestPath)
		if err != nil {
			return fmt.Errorf("loading initial manifest: %w", err)
		}

		pubSub, err := pubsub.NewGossipSub(c.Context, host,
			pubsub.WithPeerExchange(true),
			pubsub.WithFloodPublish(true),
			pubsub.WithPeerScore(psutil.PubsubPeerScoreParams, psutil.PubsubPeerScoreThresholds),
		)
		if err != nil {
			return fmt.Errorf("initialzing pubsub: %w", err)
		}

		sender, err := manifest.NewManifestSender(c.Context, host, pubSub, currentManifest, c.Duration("publishInterval"))
		if err != nil {
			return fmt.Errorf("initialzing manifest sender: %w", err)
		}
		_, _ = fmt.Fprintf(c.App.Writer, "Started manifest sender with network name: %s\n", currentManifest.NetworkName)

		checkInterval := c.Duration("checkInterval")

		errgrp, ctx := errgroup.WithContext(c.Context)
		errgrp.Go(func() error { return sender.Run(ctx) })
		errgrp.Go(func() error {
			checkTicker := time.NewTicker(checkInterval)
			defer checkTicker.Stop()

			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return nil
				case <-checkTicker.C:
					// Reload manifest and if changed republish.
					if nextManifest, err := loadManifest(manifestPath); err != nil {
						_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed reload manifest: %v\n", err)
					} else if !nextManifest.Equal(currentManifest) {
						if mcid, err := nextManifest.Cid(); err != nil {
							_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed determine manifest CID: %v\n", err)
						} else {
							_, _ = fmt.Fprintf(c.App.Writer, "Loaded changed manifest with network name: %q, and CID %s\n", nextManifest.NetworkName, mcid)
							sender.UpdateManifest(nextManifest)
							currentManifest = nextManifest
						}
					}

					// Periodically reconnect to bootstrappers; this would reload any changes to
					// bootrsrappers file and increases the chances of having a better connected mesh
					// for gossipsub to propagate.
					connectToBootstrappers()
				}
			}

			return nil
		})

		return errgrp.Wait()
	},
}

func getManifest(c *cli.Context) (*manifest.Manifest, error) {
	manifestPath := c.String("manifest")
	return loadManifest(manifestPath)
}

func loadManifest(path string) (*manifest.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s to load manifest: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return manifest.Unmarshal(f)
}
