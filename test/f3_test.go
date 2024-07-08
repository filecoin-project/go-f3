package test

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/sim/signing"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	ManifestSenderTimeout = 1 * time.Second
	logLevel              = "info"
)

var log = logging.Logger("f3-testing")

func TestSimpleF3(t *testing.T) {
	env := newTestEnvironment(t, 2, false)

	env.connectAll()
	env.start()
	env.waitForInstanceNumber(5, 10*time.Second, false)
}

func TestDynamicManifest_WithoutChanges(t *testing.T) {
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()
	prev := env.nodes[0].f3.Manifest()

	env.waitForInstanceNumber(5, 10*time.Second, false)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)
}

func TestDynamicManifest_WithRebootstrap(t *testing.T) {
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()

	prev := env.nodes[0].f3.Manifest()
	env.waitForInstanceNumber(3, 15*time.Second, false)
	prevInstance := env.nodes[0].currentGpbftInstance()

	env.manifest.BootstrapEpoch = 1253
	env.addPowerDeltaForParticipants(&env.manifest, []gpbft.ActorID{2, 3}, big.NewInt(1), false)
	env.updateManifest()

	env.waitForManifestChange(prev, 35*time.Second)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].currentGpbftInstance() < prevInstance)
	env.waitForInstanceNumber(3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)

	// check that the power table is updated
	ts, err := env.ec.GetTipsetByEpoch(env.testCtx, int64(env.nodes[0].currentGpbftInstance()))
	require.NoError(t, err)
	pt, err := env.nodes[0].f3.GetPowerTable(env.testCtx, ts.Key())
	require.NoError(t, err)
	require.Equal(t, len(pt), 4)
}

func TestDynamicManifest_WithPauseAndRebootstrap(t *testing.T) {
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()

	prev := env.nodes[0].f3.Manifest()
	env.waitForInstanceNumber(3, 15*time.Second, false)
	prevInstance := env.nodes[0].currentGpbftInstance()

	env.manifestSender.Pause()

	env.waitForManifestChange(prev, 15*time.Second)

	// check that it paused
	env.waitForNodesStoppped(10 * time.Second)

	// New manifest with sequence 2 to start again F3
	prev = env.nodes[0].f3.Manifest()

	env.manifest.BootstrapEpoch = 956
	env.updateManifest()

	env.waitForManifestChange(prev, 15*time.Second)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].currentGpbftInstance() < prevInstance)
	env.waitForInstanceNumber(3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)
}

var base manifest.Manifest = manifest.Manifest{
	BootstrapEpoch:  950,
	InitialInstance: 0,
	NetworkName:     gpbft.NetworkName("f3-test"),
	GpbftConfig: &manifest.GpbftConfig{
		// Added a higher delta and lower backoff exponent so tests run faster
		Delta:                500 * time.Millisecond,
		DeltaBackOffExponent: 1,
		MaxLookaheadRounds:   5,
	},
	// EcConfig:        manifest.DefaultEcConfig,
	EcConfig: &manifest.EcConfig{
		ECFinality:       10,
		CommiteeLookback: 5,
		// increased delay and period to accelerate test times.
		ECPeriod:                 100 * time.Millisecond,
		ECDelayMultiplier:        1.0,
		BaseDecisionBackoffTable: []float64{1., 1.2},
	},
}

type testNode struct {
	e  *testEnv
	h  host.Host
	f3 *f3.F3
}

func (n *testNode) currentGpbftInstance() uint64 {
	c, err := n.f3.GetLatestCert(n.e.testCtx)
	require.NoError(n.e.t, err)
	if c == nil {
		return 0
	}
	return c.GPBFTInstance
}

type testEnv struct {
	t              *testing.T
	errgrp         *errgroup.Group
	testCtx        context.Context
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *ec.FakeEC
	manifestSender *manifest.ManifestSender
	net            mocknet.Mocknet

	manifest manifest.Manifest
}

// signals the update to the latest manifest in the environment.
func (e *testEnv) updateManifest() {
	m := e.manifest // copy because we mutate it locally.
	e.manifestSender.UpdateManifest(&m)
}

func (e *testEnv) newHeadEveryPeriod(period time.Duration) {
	e.errgrp.Go(func() error {
		// set a timer that sets a new head every period
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for e.testCtx.Err() == nil {
			select {
			case <-e.testCtx.Done():
				return nil
			case now := <-ticker.C:
				// Catchup in case we fall behind.
				for {
					h, err := e.ec.GetHead(e.testCtx)
					if err != nil {
						return err
					}
					if !h.Timestamp().Before(now) {
						break
					}
					e.ec.SetCurrentHead(e.ec.GetCurrentHead() + 1)
				}
			}
		}
		return nil
	})
}

func (e *testEnv) addPowerDeltaForParticipants(m *manifest.Manifest, participants []gpbft.ActorID, power *big.Int, runNodes bool) {
	for _, n := range participants {
		nodeLen := len(e.nodes)
		newNode := false
		// nodes are initialized sequentially. If the participantID is over the
		// number of nodes, it means that it hasn't been initialized and is a new node
		// that we need to add
		// If the ID matches an existing one, it adds to the existing power.
		// NOTE: We do not respect the original ID when adding a new nodes, we use the subsequent one.
		if n >= gpbft.ActorID(nodeLen) {
			e.initNode(int(nodeLen), e.manifestSender.SenderID())
			newNode = true
		}
		pubkey, _ := e.signingBackend.GenerateKey()
		m.PowerUpdate = append(m.PowerUpdate, certs.PowerTableDelta{
			ParticipantID: gpbft.ActorID(nodeLen),
			SigningKey:    pubkey,
			PowerDelta:    power,
		})
		if runNodes && newNode {
			// connect node
			for j := 0; j < nodeLen-1; j++ {
				_, err := e.net.LinkPeers(e.nodes[nodeLen-1].h.ID(), e.nodes[j].h.ID())
				require.NoError(e.t, err)
				_, err = e.net.ConnectPeers(e.nodes[nodeLen-1].h.ID(), e.nodes[j].h.ID())
				require.NoError(e.t, err)
			}
			// run
			e.startNode(nodeLen - 1)
		}
	}
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForInstanceNumber(instanceNumber uint64, timeout time.Duration, strict bool) {
	require.Eventually(e.t, func() bool {
		for _, n := range e.nodes {
			// nodes that are not running are not required to reach the instance
			// (it will actually panic if we try to fetch it because there is no
			// runner initialized)
			if !n.f3.IsRunning() {
				if strict {
					return false
				}
				continue
			}
			if n.currentGpbftInstance() < instanceNumber {
				return false
			}
		}
		return true
	}, timeout, e.manifest.ECPeriod)
}

func (e *testEnv) waitForManifestChange(prev *manifest.Manifest, timeout time.Duration) {
	require.Eventually(e.t, func() bool {
		oldVersion, err := prev.Version()
		require.NoError(e.t, err)
		for _, n := range e.nodes {
			if !n.f3.IsRunning() {
				continue
			}

			m := n.f3.Manifest()
			if m == nil {
				return false
			}

			v, err := m.Version()
			require.NoError(e.t, err)
			if v == oldVersion {
				return false
			}
		}
		return true
	}, timeout, ManifestSenderTimeout)
}

func newTestEnvironment(t *testing.T, n int, dynamicManifest bool) testEnv {
	ctx, cancel := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)
	env := testEnv{t: t, errgrp: grp, testCtx: ctx, net: mocknet.New()}
	env.t.Cleanup(func() {
		cancel()
		require.NoError(env.t, env.errgrp.Wait())
	})

	// populate manifest
	m := base
	initialPowerTable := gpbft.PowerEntries{}

	env.signingBackend = signing.NewFakeBackend()
	for i := 0; i < n; i++ {
		pubkey, _ := env.signingBackend.GenerateKey()

		initialPowerTable = append(initialPowerTable, gpbft.PowerEntry{
			ID:     gpbft.ActorID(i),
			PubKey: pubkey,
			Power:  big.NewInt(1000),
		})
	}
	env.manifest = m
	env.ec = ec.NewFakeEC(1, m.BootstrapEpoch+m.ECFinality, m.ECPeriod, initialPowerTable, false)
	env.ec.SetCurrentHead(m.BootstrapEpoch)

	var manifestServer peer.ID
	if dynamicManifest {
		env.newManifestSender()
		manifestServer = env.manifestSender.SenderID()
	}

	// initialize nodes
	for i := 0; i < n; i++ {
		env.initNode(i, manifestServer)
	}
	return env
}

func (e *testEnv) initNode(i int, manifestServer peer.ID) {
	n, err := e.newF3Instance(i, manifestServer)
	require.NoError(e.t, err)
	e.nodes = append(e.nodes, n)
}

func (e *testEnv) requireEqualManifests(strict bool) {
	m := e.nodes[0].f3.Manifest()
	for _, n := range e.nodes {
		// only check running nodes
		if n.f3.IsRunning() || strict {
			require.Equal(e.t, n.f3.Manifest(), m)
		}
	}
}

func (e *testEnv) waitFor(f func(n *testNode) bool, timeout time.Duration) {
	require.Eventually(e.t, func() bool {
		reached := 0
		for i := 0; i < len(e.nodes); i++ {
			if f(e.nodes[i]) {
				reached++
			}
			if reached == len(e.nodes) {
				return true
			}
		}
		return false
	}, timeout, e.manifest.ECPeriod)
}

func (e *testEnv) waitForNodesInitialization() {
	f := func(n *testNode) bool {
		return n.f3.IsRunning()
	}
	e.waitFor(f, 5*time.Second)
}

func (e *testEnv) waitForNodesStoppped(timeout time.Duration) {
	f := func(n *testNode) bool {
		return !n.f3.IsRunning()
	}
	e.waitFor(f, timeout)
}

func (e *testEnv) start() {
	// Start the nodes
	for i := range e.nodes {
		e.startNode(i)
	}

	// wait for nodes to initialize
	e.waitForNodesInitialization()

	// If it exists, start the manifest sender
	if e.manifestSender != nil {
		e.errgrp.Go(func() error { return e.manifestSender.Run(e.testCtx) })
	}

	// start creating new heads every ECPeriod
	e.newHeadEveryPeriod(e.manifest.ECPeriod)
}

func (e *testEnv) startNode(i int) {
	n := e.nodes[i]
	require.NoError(e.t, n.f3.Start(e.testCtx))
	e.t.Cleanup(func() {
		require.NoError(e.t, n.f3.Stop(context.Background()))
	})
}

func (e *testEnv) connectAll() {
	for i, n := range e.nodes {
		for j := i + 1; j < len(e.nodes); j++ {
			_, err := e.net.LinkPeers(n.h.ID(), e.nodes[j].h.ID())
			require.NoError(e.t, err)
			_, err = e.net.ConnectPeers(n.h.ID(), e.nodes[j].h.ID())
			require.NoError(e.t, err)
		}
	}

	// connect to the manifest server if it exists
	if e.manifestSender != nil {
		id := e.manifestSender.PeerInfo().ID
		for _, n := range e.nodes {
			_, err := e.net.LinkPeers(n.h.ID(), id)
			require.NoError(e.t, err)
			_, err = e.net.ConnectPeers(n.h.ID(), id)
			require.NoError(e.t, err)
		}

	}
}

func (e *testEnv) newManifestSender() {
	h, err := e.net.GenPeer()
	require.NoError(e.t, err)

	ps, err := pubsub.NewGossipSub(e.testCtx, h)
	require.NoError(e.t, err)

	m := e.manifest // copy because we mutate this
	e.manifestSender, err = manifest.NewManifestSender(h, ps, &m, ManifestSenderTimeout)
	require.NoError(e.t, err)
}

func (e *testEnv) newF3Instance(id int, manifestServer peer.ID) (*testNode, error) {
	h, err := e.net.GenPeer()
	if err != nil {
		return nil, xerrors.Errorf("creating libp2p host: %w", err)
	}

	ps, err := pubsub.NewGossipSub(e.testCtx, h)
	if err != nil {
		return nil, xerrors.Errorf("creating gossipsub: %w", err)
	}

	tmpdir, err := os.MkdirTemp("", "f3-*")
	if err != nil {
		return nil, xerrors.Errorf("creating temp dir: %w", err)
	}

	err = logging.SetLogLevel("f3-testing", logLevel)
	if err != nil {
		return nil, xerrors.Errorf("setting log level: %w", err)
	}

	ds, err := leveldb.NewDatastore(tmpdir, nil)
	if err != nil {
		return nil, xerrors.Errorf("creating a datastore: %w", err)
	}

	m := e.manifest // copy because we mutate this
	var mprovider manifest.ManifestProvider
	if manifestServer != peer.ID("") {
		mprovider = manifest.NewDynamicManifestProvider(&m, ps, e.ec, manifestServer)
	} else {
		mprovider = manifest.NewStaticManifestProvider(&m)
	}

	e.signingBackend.Allow(int(id))

	module, err := f3.New(e.testCtx, mprovider, ds, h, ps, e.signingBackend, e.ec, nil)
	if err != nil {
		return nil, xerrors.Errorf("creating module: %w", err)
	}

	e.errgrp.Go(func() error {
		return runMessageSubscription(e.testCtx, module, gpbft.ActorID(id), e.signingBackend)
	})

	return &testNode{e: e, h: h, f3: module}, nil
}

// TODO: This code is copy-pasta from cmd/f3/run.go, consider taking it out into a shared testing lib.
// We could do the same to the F3 test instantiation
func runMessageSubscription(ctx context.Context, module *f3.F3, actorID gpbft.ActorID, signer gpbft.Signer) error {
	ch := make(chan *gpbft.MessageBuilder, 4)
	module.SubscribeForMessagesToSign(ch)
	for ctx.Err() == nil {
		select {
		case mb, ok := <-ch:
			if !ok {
				// the broadcast bus kicked us out
				log.Infof("lost message bus subscription, retrying")
				ch = make(chan *gpbft.MessageBuilder, 4)
				module.SubscribeForMessagesToSign(ch)
				continue
			}
			signatureBuilder, err := mb.PrepareSigningInputs(actorID)
			if err != nil {
				return xerrors.Errorf("preparing signing inputs: %w", err)
			}
			// signatureBuilder can be sent over RPC
			payloadSig, vrfSig, err := signatureBuilder.Sign(ctx, signer)
			if err != nil {
				return xerrors.Errorf("signing message: %w", err)
			}
			// signatureBuilder and signatures can be returned back over RPC
			module.Broadcast(ctx, signatureBuilder, payloadSig, vrfSig)
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
