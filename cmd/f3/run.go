package main

import (
	"context"
	"os"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/signing"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

const DiscoveryTag = "f3-standalone"

var log = logging.Logger("f3")

var runCmd = cli.Command{
	Name:  "run",
	Usage: "starts a f3 node",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "id",
			Value: 0,
		},
	},
	Action: func(c *cli.Context) error {
		ctx := c.Context
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/quic-v1"))
		if err != nil {
			return xerrors.Errorf("creating libp2p host: %w", err)
		}

		ps, err := pubsub.NewGossipSub(ctx, h)
		if err != nil {
			return xerrors.Errorf("creating gossipsub: %w", err)
		}

		closer, err := setupDiscovery(h)
		if err != nil {
			return xerrors.Errorf("setting up discovery: %w", err)
		}
		defer closer()

		tmpdir, err := os.MkdirTemp("", "f3-*")
		if err != nil {
			return xerrors.Errorf("creating temp dir: %w", err)
		}

		ds, err := leveldb.NewDatastore(tmpdir, nil)
		if err != nil {
			return xerrors.Errorf("creating a datastore: %w", err)
		}

		m, err := getManifest(c)
		if err != nil {
			return xerrors.Errorf("loading manifest: %w", err)
		}

		err = logging.SetLogLevel("f3", "debug")
		if err != nil {
			return xerrors.Errorf("setting log level: %w", err)
		}

		signingBackend := signing.NewFakeBackend()
		id := c.Uint64("id")
		signingBackend.Allow(int(id))
		module, err := f3.NewModule(ctx, gpbft.ActorID(id), m, ds, h, ps, signingBackend, signingBackend, nil, log)
		if err != nil {
			return xerrors.Errorf("creating module: %w", err)
		}

		return module.Run(ctx)
	},
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
