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
		&cli.Uint64Flag{
			Name:  "instance",
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

		signingBackend := &fakeSigner{*signing.NewFakeBackend()}
		id := c.Uint64("id")
		signingBackend.Allow(int(id))

		ec := NewFakeEC(1, m)

		module, err := f3.New(ctx, gpbft.ActorID(id), m, ds, h, ps,
			signingBackend, ec, log, nil)
		if err != nil {
			return xerrors.Errorf("creating module: %w", err)
		}

		actorID := gpbft.ActorID(c.Uint64("id"))

		go runMessageSubscription(ctx, module, actorID, signingBackend)

		return module.Run(ctx)
	},
}

func runMessageSubscription(ctx context.Context, module *f3.F3, actorID gpbft.ActorID, signer gpbft.Signer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch := make(chan *gpbft.MessageBuilder, 4)
		module.SubscribeForMessagesToSign(ch)
	inner:
		for {
			select {
			case mb, ok := <-ch:
				if !ok {
					// the broadcast bus kicked us out
					log.Infof("lost message bus subscription, retrying")
					break inner
				}
				signatureBuilder, err := mb.PrepareSigningInputs(actorID)
				if err != nil {
					log.Errorf("preparing signing inputs: %+v", err)
				}
				// signatureBuilder can be sent over RPC
				payloadSig, vrfSig, err := signatureBuilder.Sign(signer)
				if err != nil {
					log.Errorf("signing message: %+v", err)
				}
				// signatureBuilder and signatures can be returned back over RPC
				module.Broadcast(ctx, signatureBuilder, payloadSig, vrfSig)
			case <-ctx.Done():
				return
			}
		}

	}
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

// MarshalPayloadForSigning marshals the given payload into the bytes that should be signed.
// This should usually call `Payload.MarshalForSigning(NetworkName)` except when testing as
// that method is slow (computes a merkle tree that's necessary for testing).
// Implementations must be safe for concurrent use.
func (fs *fakeSigner) MarshalPayloadForSigning(nn gpbft.NetworkName, p *gpbft.Payload) []byte {
	return p.MarshalForSigning(nn)
}
