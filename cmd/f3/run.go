package main

import (
	"context"
	"math/big"
	"os"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
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

		// setup initial power table for number of participants
		initialPowerTable := []gpbft.PowerEntry{}
		fsig := signing.NewFakeBackend()
		for i := 0; i < c.Int("N"); i++ {
			pubkey, _ := fsig.GenerateKey()

			initialPowerTable = append(initialPowerTable, gpbft.PowerEntry{
				ID:     gpbft.ActorID(i),
				PubKey: pubkey,
				Power:  big.NewInt(1000),
			})
		}

		// if the manifest-server ID is passed in a flag,
		// we setup the monitoring system
		mFlag := c.String("manifest-server")
		var manifestServer peer.ID
		var mprovider manifest.ManifestProvider
		if mFlag != "" {
			manifestServer, err = peer.Decode(mFlag)
			if err != nil {
				return xerrors.Errorf("parsing manifest server ID: %w", err)
			}
			mprovider = manifest.NewDynamicManifestProvider(m, ps, nil, manifestServer)
		} else {
			mprovider = manifest.NewStaticManifestProvider(m)
		}

		signingBackend := &fakeSigner{*signing.NewFakeBackend()}
		id := c.Uint64("id")
		signingBackend.Allow(int(id))

		ec := ec.NewFakeEC(1, m.BootstrapEpoch, m.ECPeriod, initialPowerTable, true)

		module, err := f3.New(ctx, mprovider, ds, h, ps, signingBackend, ec)
		if err != nil {
			return xerrors.Errorf("creating module: %w", err)
		}

		go runMessageSubscription(ctx, module, gpbft.ActorID(id), signingBackend)

		if err := module.Start(ctx); err != nil {
			return nil
		}
		<-ctx.Done()
		return module.Stop(context.Background())
	},
}

func runMessageSubscription(ctx context.Context, module *f3.F3, actorID gpbft.ActorID, signer gpbft.Signer) {
	for ctx.Err() == nil {

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
				payloadSig, vrfSig, err := signatureBuilder.Sign(ctx, signer)
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
