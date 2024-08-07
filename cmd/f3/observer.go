package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var observerCmd = cli.Command{
	Name:  "observe",
	Usage: "observes and logs messages in F3 network",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:  "identity",
			Usage: "The path to protobuf encoded libp2p identity of the server.",
			Value: "./observer/identity",
		},
		&cli.StringFlag{
			Name:  "manifestID",
			Usage: "PeerID of the manifest server",
		},
		&cli.StringSliceFlag{
			Name:  "listenAddr",
			Usage: "The libp2p listen addrs.",
			Value: cli.NewStringSlice(
				"/ip4/0.0.0.0/tcp/45002",
				"/ip4/0.0.0.0/udp/45002/quic-v1",
				"/ip4/0.0.0.0/udp/45002/quic-v1/webtransport",
				"/ip6/::/tcp/45002",
				"/ip6/::/udp/45002/quic-v1",
				"/ip6/::/udp/45002/quic-v1/webtransport",
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
	},

	Action: func(c *cli.Context) error {
		logging.SetLogLevel("f3/cli", "debug")

		ds, err := leveldb.NewDatastore("./observer/db", nil)
		defer ds.Close()
		if err != nil {
			return fmt.Errorf("opening datastore: %w", err)
		}

		var id crypto.PrivKey

		enId, err := os.ReadFile(c.String("identity"))
		if errors.Is(err, fs.ErrNotExist) {
			_, _ = fmt.Fprintln(c.App.Writer, "identity file does not exist, generating a new one")
			var err error
			id, _, err = crypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				return fmt.Errorf("generating random libp2p identity: %w", err)
			}
			keyBytes, err := crypto.MarshalPrivateKey(id)
			if err != nil {
				return fmt.Errorf("marshalling key: %w", err)
			}
			enId = keyBytes

			err = os.WriteFile(c.String("identity"), keyBytes, 0600)
			if err != nil {
				return fmt.Errorf("writing key: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("reading libp2p identity file: %w", err)
		}
		id, err = crypto.UnmarshalPrivateKey(enId)
		if err != nil {
			return fmt.Errorf("unmarshalling libp2p identity: %w", err)
		}

		peerID, err := peer.IDFromPrivateKey(id)
		if err != nil {
			return fmt.Errorf("getting peer ID from libp2p identity: %w", err)
		}

		_, _ = fmt.Fprintf(c.App.Writer, "Observer peer ID: %s\n", peerID)

		host, err := libp2p.New(
			libp2p.Identity(id),
			libp2p.ListenAddrStrings(c.StringSlice("listenAddr")...),
			libp2p.UserAgent("f3-observer"),
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
				addr, err := peer.AddrInfoFromString(bootstrapper)
				if err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to parse to bootstrap address: %s %v\n", bootstrapper, err)
				} else if err := host.Connect(c.Context, *addr); err != nil {
					_, _ = fmt.Fprintf(c.App.ErrWriter, "Failed to connect to bootstrap address: %v\n", err)
				}
			}
			log.Infof("connected to boostrap")
		}

		// Connect to bootstrappers once as soon as we start.
		connectToBootstrappers()

		pubSub, err := pubsub.NewGossipSub(c.Context, host, pubsub.WithPeerExchange(true))
		if err != nil {
			return fmt.Errorf("initialzing pubsub: %w", err)
		}

		manifestServerID, err := peer.Decode(c.String("manifestID"))
		if err != nil {
			return fmt.Errorf("decoding manifestServerID: %w", err)
		}
		manifestProvider := manifest.NewDynamicManifestProvider(&manifest.Manifest{}, ds, pubSub, manifestServerID)
		if err != nil {
			return fmt.Errorf("initialzing manifest sender: %w", err)
		}
		err = manifestProvider.Start(c.Context)
		if err != nil {
			return fmt.Errorf("starting manifest provider: %w", err)
		}

		var closer func()
		for c.Context.Err() == nil {
			select {
			case manif := <-manifestProvider.ManifestUpdates():
				log.Infof("Got manifest for network %s\n", manif.NetworkName)
				if closer != nil {
					closer()
				}

				networkCtx, c := context.WithCancel(c.Context)
				_ = networkCtx
				closer = c
				err := observeManifest(networkCtx, manif, pubSub)
				if err != nil {
					log.Errorw("starting manifest observer", "error", err, "networkName", manif.NetworkName)
				}
			case <-c.Context.Done():
			}
		}
		return nil
	},
}

func observeManifest(ctx context.Context, manif *manifest.Manifest, pubSub *pubsub.PubSub) error {
	err := pubSub.RegisterTopicValidator(manif.PubSubTopic(), func(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		var gmsg gpbft.GMessage
		if err := gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			log.Errorf("rejecting due to: %v", err)
			return pubsub.ValidationReject
		}
		//msg.ValidatorData = gmsg

		return pubsub.ValidationAccept
	})
	if err != nil {
		return fmt.Errorf("registering topic validator: %v", err)
	}

	topic, err := pubSub.Join(manif.PubSubTopic(), pubsub.WithTopicMessageIdFn(pubsub.DefaultMsgIdFn))
	if err != nil {
		return fmt.Errorf("joining topic")
	}

	log.Info("pre subscribe")
	sub, err := topic.Subscribe()
	log.Info("post subscribe")
	if err != nil {
		return fmt.Errorf("subscribing to topic")
	}

	go func() {
		for ctx.Err() == nil {
			log.Infof("sub.Next")
			msg, err := sub.Next(ctx)
			log.Infof("sub.Next post")
			if err != nil {
				if ctx.Err() != nil {
					log.Errorf("existing due to context")
					return
				}
				log.Errorf("getting a message from pubsub: %v", err)
			}
			log.Infof("got a msg from %d", msg.ID)
		}
	}()
	return nil
}
