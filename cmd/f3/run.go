package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/filecoin-project/go-f3/sim/signing"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("f3/cli")

const DiscoveryTag = "f3-standalone"

var runCmd = cli.Command{
	Name:  "run",
	Usage: "starts a f3 node",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "id",
			Value: 0,
		},
		&cli.Uint64Flag{
			Name:  "instance",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "N",
			Usage: "number of participant. Should be the same in all nodes as it influences the initial power table",
			Value: 2,
		},
	},
	Action: func(c *cli.Context) error {
		ctx := c.Context
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/quic-v1"))
		if err != nil {
			return fmt.Errorf("creating libp2p host: %w", err)
		}

		ps, err := pubsub.NewGossipSub(ctx, h)
		if err != nil {
			return fmt.Errorf("creating gossipsub: %w", err)
		}

		closer, err := setupDiscovery(h)
		if err != nil {
			return fmt.Errorf("setting up discovery: %w", err)
		}
		defer closer()

		tmpdir, err := os.MkdirTemp("", "f3-*")
		if err != nil {
			return fmt.Errorf("creating temp dir: %w", err)
		}

		ds, err := leveldb.NewDatastore(filepath.Join(tmpdir, "datastore"), nil)
		if err != nil {
			return fmt.Errorf("creating a datastore: %w", err)
		}

		m, err := getManifest(c)
		if err != nil {
			return fmt.Errorf("loading manifest: %w", err)
		}

		err = logging.SetLogLevel("f3", "debug")
		if err != nil {
			return fmt.Errorf("setting log level: %w", err)
		}

		// setup initial power table for number of participants
		var initialPowerTable []gpbft.PowerEntry
		fsig := signing.NewFakeBackend()
		for i := 0; i < c.Int("N"); i++ {
			pubkey, _ := fsig.GenerateKey()

			initialPowerTable = append(initialPowerTable, gpbft.PowerEntry{
				ID:     gpbft.ActorID(i),
				PubKey: pubkey,
				Power:  gpbft.NewStoragePower(1000),
			})
		}

		signingBackend := &fakeSigner{*signing.NewFakeBackend()}
		id := c.Uint64("id")
		signingBackend.Allow(int(id))

		ec := consensus.NewFakeEC(
			consensus.WithBootstrapEpoch(m.BootstrapEpoch),
			consensus.WithECPeriod(m.EC.Period),
			consensus.WithInitialPowerTable(initialPowerTable),
		)

		module, err := f3.New(ctx, m, ds, h, ps, signingBackend, ec, filepath.Join(tmpdir, "f3"))
		if err != nil {
			return fmt.Errorf("creating module: %w", err)
		}

		errCh := make(chan error, 1)
		go func() { errCh <- runMessageSubscription(ctx, module, gpbft.ActorID(id), signingBackend) }()

		if err := module.Start(ctx); err != nil {
			return nil
		}
		select {
		case err := <-errCh:
			if err != nil {
				log.Error(err)
			}
		case <-ctx.Done():
		}
		return module.Stop(context.Background())
	},
}

func runMessageSubscription(ctx context.Context, module *f3.F3, actorID gpbft.ActorID, signer gpbft.Signer) error {
	for ctx.Err() == nil {
		select {
		case mb, ok := <-module.MessagesToSign():
			if !ok {
				return nil
			}
			signatureBuilder, err := mb.PrepareSigningInputs(actorID)
			if err != nil {
				return fmt.Errorf("preparing signing inputs: %w", err)
			}
			// signatureBuilder can be sent over RPC
			payloadSig, vrfSig, err := signatureBuilder.Sign(ctx, signer)
			if err != nil {
				return fmt.Errorf("signing message: %w", err)
			}
			// signatureBuilder and signatures can be returned back over RPC
			module.Broadcast(ctx, signatureBuilder, payloadSig, vrfSig)
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

type discoveryNotifee struct {
	h host.Host
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Infof("discovered new peer %s\n", pi.ID)
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		log.Infof("error connecting to peer %s: %s\n", pi.ID, err)
	}
}

func setupDiscovery(h host.Host) (closer func(), err error) {
	// setup mDNS discovery to find local peers
	s := mdns.NewMdnsService(h, DiscoveryTag, &discoveryNotifee{h: h})
	return func() { s.Close() }, s.Start()
}

type fakeSigner struct {
	signing.FakeBackend
}
