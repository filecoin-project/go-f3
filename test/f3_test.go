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
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

const ManifestSenderTimeout = 1 * time.Second

var log = logging.Logger("f3-testing")

func TestSimpleF3(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, false)

	env.Connect(ctx)
	env.run(ctx)
	env.waitForInstanceNumber(ctx, 5, 10*time.Second, false)
}

func TestDynamicManifest_WithoutChanges(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.run(ctx)
	prev := env.nodes[0].f3.Manifest.Manifest()

	env.waitForInstanceNumber(ctx, 5, 10*time.Second, false)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
}

func TestDynamicManifest_WithRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.run(ctx)

	prev := env.nodes[0].f3.Manifest.Manifest()
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	prevInstance := env.nodes[0].CurrentGpbftInstance(t, ctx)

	env.manifest.BootstrapEpoch = 953
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = true
	env.updateManifest()

	env.waitForManifestChange(prev, 15*time.Second, env.nodes)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].CurrentGpbftInstance(t, ctx) < prevInstance)
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
}

func TestDynamicManifest_SubsequentWithRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.run(ctx)

	prev := env.nodes[0].f3.Manifest.Manifest()
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	prevInstance := env.nodes[0].CurrentGpbftInstance(t, ctx)

	env.manifest.BootstrapEpoch = 953
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = true
	env.updateManifest()

	env.waitForManifestChange(prev, 15*time.Second, env.nodes)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].CurrentGpbftInstance(t, ctx) < prevInstance)
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

	// New manifest with sequence 2
	prev = env.nodes[0].f3.Manifest.Manifest()
	prevInstance = env.nodes[0].CurrentGpbftInstance(t, ctx)

	env.manifest.BootstrapEpoch = 956
	env.manifest.Sequence = 2
	env.manifest.ReBootstrap = true
	env.updateManifest()

	env.waitForManifestChange(prev, 15*time.Second, env.nodes)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].CurrentGpbftInstance(t, ctx) < prevInstance)
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

	// Using a manifest with a lower sequence number doesn't trigger an update
	prev = env.nodes[0].f3.Manifest.Manifest()
	env.manifest.BootstrapEpoch = 965
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = true
	env.updateManifest()

	// check that no manifest change has propagated
	require.Equal(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

}

func TestDynamicManifest_WithoutRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.run(ctx)

	prev := env.nodes[0].f3.Manifest.Manifest()
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	prevInstance := env.nodes[0].CurrentGpbftInstance(t, ctx)

	env.manifest.BootstrapEpoch = 953
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = false
	env.addPowerDeltaForParticipants(ctx, &env.manifest, []gpbft.ActorID{2, 3}, big.NewInt(1), false)
	env.updateManifest()

	env.waitForManifestChange(prev, 15*time.Second, []*testNode{env.nodes[0], env.nodes[1]})

	// check that the runner continued without rebootstrap
	require.True(t, env.nodes[0].CurrentGpbftInstance(t, ctx) >= prevInstance)
	env.waitForInstanceNumber(ctx, prevInstance+10, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
	// check that the power table is updated with the new entries
	ts, err := env.ec.GetTipsetByEpoch(ctx, int64(env.nodes[0].CurrentGpbftInstance(t, ctx)))
	require.NoError(t, err)
	pt, err := env.nodes[0].f3.GetPowerTable(ctx, ts.Key())
	require.NoError(t, err)
	require.Equal(t, len(pt), 4)
}

var base manifest.Manifest = manifest.Manifest{
	Sequence:        0,
	BootstrapEpoch:  950,
	ReBootstrap:     true,
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
		ECDelay:  500 * time.Millisecond,
		ECPeriod: 500 * time.Millisecond,
	},
}

type testNode struct {
	h     host.Host
	f3    *f3.F3
	errCh <-chan error
}

func (n *testNode) CurrentGpbftInstance(t *testing.T, ctx context.Context) uint64 {
	c, err := n.f3.GetLatestCert(ctx)
	require.NoError(t, err)
	if c == nil {
		return 0
	}
	return c.GPBFTInstance
}

type testEnv struct {
	t              *testing.T
	manifest       manifest.Manifest
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *ec.FakeEC

	manifestSender *manifest.ManifestSender
}

// signals the update to the latest manifest in the environment.
func (e *testEnv) updateManifest() {
	e.manifestSender.UpdateManifest(&e.manifest)
}

func (e *testEnv) newHeadEveryPeriod(ctx context.Context, period time.Duration) {
	// set a timer that sets a new head every period
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(period):
				e.ec.SetCurrentHead(e.ec.GetCurrentHead() + 1)
				// fmt.Println("Setting new head", e.ec.GetCurrentHead())
			}
		}
	}()
}

func (e *testEnv) addPowerDeltaForParticipants(ctx context.Context, m *manifest.Manifest, participants []gpbft.ActorID, power *big.Int, runNodes bool) {
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
				e.connectNodes(ctx, e.nodes[nodeLen-1], e.nodes[j])
			}
			// run
			go e.runNode(ctx, e.nodes[nodeLen-1])
		}
	}
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForInstanceNumber(ctx context.Context, instanceNumber uint64, timeout time.Duration, strict bool) {
	require.Eventually(e.t, func() bool {
		reached := 0
		for i := 0; i < len(e.nodes); i++ {
			// nodes that are not running are not required to reach the instance
			// (it will actually panic if we try to fetch it because there is no
			// runner initialized)
			if !e.nodes[i].f3.IsRunning() && !strict {
				reached++
			} else if e.nodes[i].CurrentGpbftInstance(e.t, ctx) >= instanceNumber && e.nodes[i].f3.IsRunning() {
				reached++
			}
			if reached >= len(e.nodes) {
				return true
			}
		}
		return false
	}, timeout, e.manifest.ECPeriod)
}

func (e *testEnv) waitForManifestChange(prev manifest.Manifest, timeout time.Duration, nodes []*testNode) {
	require.Eventually(e.t, func() bool {
		reached := 0
		for i := 0; i < len(e.nodes); i++ {
			for i := 0; i < len(nodes); i++ {
				v1, _ := nodes[i].f3.Manifest.Manifest().Version()
				v2, _ := prev.Version()
				if v1 != v2 {
					reached++
				}
				if reached == len(nodes) {
					return true
				}
			}
		}
		return false
	}, timeout, ManifestSenderTimeout)
}

func newTestEnvironment(t *testing.T, n int, dynamicManifest bool) testEnv {
	env := testEnv{t: t}

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
		env.newManifestSender(context.Background())
		manifestServer = env.manifestSender.SenderID()
	}

	// initialize nodes
	for i := 0; i < n; i++ {
		env.initNode(i, manifestServer)
	}
	return env
}

func (e *testEnv) initNode(i int, manifestServer peer.ID) {
	n, err := e.newF3Instance(context.Background(), i, manifestServer)
	require.NoError(e.t, err)
	e.nodes = append(e.nodes, n)
}

func (e *testEnv) requireEqualManifests(strict bool) {
	m := e.nodes[0].f3.Manifest
	for _, n := range e.nodes {
		// only check running nodes
		if n.f3.IsRunning() || strict {
			require.Equal(e.t, n.f3.Manifest.Manifest(), m.Manifest())
		}
	}
}

func (e *testEnv) waitForNodesInitialization() {
	require.Eventually(e.t, func() bool {
		reached := 0
		for i := 0; i < len(e.nodes); i++ {
			if e.nodes[i].f3.IsRunning() {
				reached++
			}
			if reached == len(e.nodes) {
				return true
			}
		}
		return false
	}, 5*time.Second, e.manifest.ECPeriod)
}

func (e *testEnv) runNode(ctx context.Context, n *testNode) {
	errCh := make(chan error)
	n.errCh = errCh
	err := n.f3.Run(ctx)
	errCh <- err
}

func (e *testEnv) run(ctx context.Context) {
	// Start the nodes
	for _, n := range e.nodes {
		go func(n *testNode) {
			e.runNode(ctx, n)
		}(n)
	}

	// wait for nodes to initialize
	e.waitForNodesInitialization()

	// If it exists, start the manifest sender
	if e.manifestSender != nil {
		go func() {
			e.manifestSender.Start(ctx)
		}()
	}

	// start creating new heads every ECPeriod
	e.newHeadEveryPeriod(ctx, e.manifest.ECPeriod)
	e.monitorNodesError(ctx)
}

func (e *testEnv) monitorNodesError(ctx context.Context) {
	for _, n := range e.nodes {
		go func(n *testNode) {
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-n.errCh:
					require.NoError(e.t, err)
				}
			}
		}(n)
	}
}

func (e *testEnv) connectNodes(ctx context.Context, n1, n2 *testNode) {
	pi := n2.h.Peerstore().PeerInfo(n2.h.ID())
	err := n1.h.Connect(ctx, pi)
	require.NoError(e.t, err)
}

func (e *testEnv) Connect(ctx context.Context) {
	for i, n := range e.nodes {
		for j := i + 1; j < len(e.nodes); j++ {
			e.connectNodes(ctx, n, e.nodes[j])
		}
	}

	// connect to the manifest server if it exists
	if e.manifestSender != nil {
		for _, n := range e.nodes {
			pi := e.manifestSender.PeerInfo()
			err := n.h.Connect(ctx, pi)
			require.NoError(e.t, err)
		}

	}
}

func (e *testEnv) newManifestSender(ctx context.Context) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/quic-v1"))
	require.NoError(e.t, err)

	ps, err := pubsub.NewGossipSub(ctx, h)
	require.NoError(e.t, err)

	e.manifestSender, err = manifest.NewManifestSender(h, ps, &e.manifest, ManifestSenderTimeout)
	require.NoError(e.t, err)
}

func (e *testEnv) newF3Instance(ctx context.Context, id int, manifestServer peer.ID) (*testNode, error) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/quic-v1"))
	if err != nil {
		return nil, xerrors.Errorf("creating libp2p host: %w", err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, xerrors.Errorf("creating gossipsub: %w", err)
	}

	tmpdir, err := os.MkdirTemp("", "f3-*")
	if err != nil {
		return nil, xerrors.Errorf("creating temp dir: %w", err)
	}

	err = logging.SetLogLevel("f3-testing", "debug")
	if err != nil {
		return nil, xerrors.Errorf("setting log level: %w", err)
	}

	ds, err := leveldb.NewDatastore(tmpdir, nil)
	if err != nil {
		return nil, xerrors.Errorf("creating a datastore: %w", err)
	}

	var mprovider manifest.ManifestProvider
	if manifestServer != peer.ID("") {
		mprovider = manifest.NewDynamicManifestProvider(e.manifest, ps, e.ec, manifestServer)
	} else {
		mprovider = manifest.NewStaticManifestProvider(e.manifest)
	}

	e.signingBackend.Allow(int(id))

	module, err := f3.New(ctx, mprovider, ds, h, manifestServer, ps,
		e.signingBackend, e.ec, log, nil)
	if err != nil {
		return nil, xerrors.Errorf("creating module: %w", err)
	}
	mprovider.SetManifestChangeCallback(f3.ManifestChangeCallback(module))
	go runMessageSubscription(ctx, module, gpbft.ActorID(id), e.signingBackend)

	return &testNode{h: h, f3: module}, nil
}

// TODO: This code is copy-pasta from cmd/f3/run.go, consider taking it out into a shared testing lib.
// We could do the same to the F3 test instantiation
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
