package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/filecoin-project/go-f3/cmd/f3/msgdump"
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
		_ = logging.SetLogLevel("f3/cli", "debug")

		ds, err := leveldb.NewDatastore("./observer/db", nil)
		if err != nil {
			return fmt.Errorf("opening datastore: %w", err)
		}
		defer ds.Close()

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

		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for c.Context.Err() == nil {
				select {
				case <-ticker.C:
					n := len(host.Network().Peers())
					if n < 5 {
						log.Infow("low peers, bootstrapping again", "NPeers", n)
						connectToBootstrappers()
					}
				case <-c.Context.Done():
				}
			}
		}()

		var closer func()
		var runningNetwork gpbft.NetworkName
		for c.Context.Err() == nil {
			select {
			case manif := <-manifestProvider.ManifestUpdates():
				log.Infof("Got manifest for network %s\n", manif.NetworkName)
				if runningNetwork == manif.NetworkName {
					log.Infof("same network, continuing")
					continue
				}
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
	dir := msgdump.DirForNetwork("./observer", manif.NetworkName)
	parquetWriter, err := msgdump.NewParquetWriter[msgdump.ParquetEnvelope](dir)
	if err != nil {
		return fmt.Errorf("creating parquet file: %w", err)
	}
	logFileName := filepath.Join(dir, "msgs.ndjson")
	msgLogFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_CREATE, 0660)
	if err != nil {
		return fmt.Errorf("creating msg log file: %w", err)
	}
	jsonWriter := json.NewEncoder(msgLogFile)

	err = pubSub.RegisterTopicValidator(manif.PubSubTopic(), func(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
		var gmsg gpbft.GMessage
		if err := gmsg.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			return pubsub.ValidationReject
		}
		msg.ValidatorData = gmsg

		return pubsub.ValidationAccept
	})
	if err != nil {
		return fmt.Errorf("registering topic validator: %w", err)
	}

	topic, err := pubSub.Join(manif.PubSubTopic(), pubsub.WithTopicMessageIdFn(pubsub.DefaultMsgIdFn))
	if err != nil {
		return fmt.Errorf("joining topic: %w", err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return fmt.Errorf("subscribing to topic: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)

	messageCh := make(chan msgdump.GMessageEnvelope, 50)

	// log writing goroutine
	go func() {
		defer msgLogFile.Close()
		defer parquetWriter.Close()
		defer signal.Stop(sigCh)

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-sigCh:
				// rotate parquet
				err := parquetWriter.Rotate()
				log.Infof("rotating parquet file")
				if err != nil {
					log.Errorf("rotating parquet file: %v", err)
				}
			case msg := <-messageCh:
				err = jsonWriter.Encode(msg)
				if err != nil {
					log.Errorf("writing to logfile: %v", err)
					continue
				}
				parquetEnvelope, err := msgdump.ToParquet(msg)
				if err != nil {
					log.Errorf("converting envelope to parquet: %v", err)
					continue
				}
				_, err = parquetWriter.Write(parquetEnvelope)
				if err != nil {
					log.Errorf("writing to parquet: %v", err)
					continue
				}
			}
		}
	}()

	// gossip goroutine
	go func() {
		for ctx.Err() == nil {
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Errorf("got error from sub.Next(): %v", err)
				return
			}

			envelope := msgdump.GMessageEnvelope{
				UnixMicroTime: time.Now().UnixNano() / 1000,
				NetworkName:   string(manif.NetworkName),
				Message:       msg.ValidatorData.(gpbft.GMessage),
			}
			select {
			case messageCh <- envelope:
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
