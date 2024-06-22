//go:build !race

// Note: We don't run these tests with race because UnsafeCurrentInstance
// would trigger the race detector.
package test

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/passive"
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

const (
	ManifestSenderTimeout = 1 * time.Second
)

var log = logging.Logger("f3-testing")

func TestSimpleF3(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, false)

	initialInstance := uint64(0)
	env.Connect(ctx)
	env.Run(ctx, initialInstance)
	// Small wait for nodes to initialize. In the future we can probably
	// make this asynchronously
	time.Sleep(1 * time.Second)
	env.waitForInstanceNumber(ctx, 5, 10*time.Second)
}

func TestDynamicManifestWithoutChanges(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	initialInstance := uint64(0)

	env.Connect(ctx)
	env.Run(ctx, initialInstance)
	prev := env.nodes[0].f3.Manifest.Manifest()
	time.Sleep(1 * time.Second)

	env.waitForInstanceNumber(ctx, 5, 10*time.Second)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests()
}

func TestDynamicManifestWithRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	initialInstance := uint64(0)

	env.Connect(ctx)
	env.Run(ctx, initialInstance)
	time.Sleep(1 * time.Second)

	prev := env.nodes[0].f3.Manifest.Manifest()

	env.waitForInstanceNumber(ctx, 3, 15*time.Second)
	prevInstance := env.nodes[0].f3.CurrentGpbftInstace()

	// Update the manifest. Bootstrap from an epoch over
	// 953 so we start receiving a new head from fakeEC
	// every second
	env.manifest.BootstrapEpoch = 953
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = true
	env.manifestSender.UpdateManifest(&env.manifest)

	env.waitForManifestChange(ctx, prev, 15*time.Second)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].f3.CurrentGpbftInstace() < prevInstance)
	env.waitForInstanceNumber(ctx, 3, 15*time.Second)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests()
}

const DiscoveryTag = "f3-standalone-testing"

var baseManifest manifest.Manifest = manifest.Manifest{
	Sequence:       0,
	BootstrapEpoch: 950,
	ReBootstrap:    true,
	NetworkName:    gpbft.NetworkName("f3-test"),
	GpbftConfig:    manifest.DefaultGpbftConfig,
	// EcConfig:       manifest.DefaultEcConfig,
	EcConfig: &manifest.EcConfig{
		ECFinality:       10,
		CommiteeLookback: 5,
		ECDelay:          1 * time.Second,

		ECPeriod: 1 * time.Second,
	},
}

type testNode struct {
	h  host.Host
	f3 *f3.F3
}

type testEnv struct {
	t              *testing.T
	manifest       manifest.Manifest
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *ec.FakeEC

	manifestSender *passive.ManifestSender
}

func (t *testEnv) waitForInstanceNumber(ctx context.Context, instanceNumber uint64, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.t.Fatal("instance number not reached before timeout")
		default:
			reached := 0
			for i := 0; i < len(t.nodes); i++ {
				if t.nodes[i].f3.CurrentGpbftInstace() >= instanceNumber {
					reached++
				}
				if reached == len(t.nodes) {
					return
				}
			}
			time.Sleep(time.Second)
		}
	}
}

func (t *testEnv) waitForManifestChange(ctx context.Context, prev manifest.Manifest, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.t.Fatal("manifest change not reached before timeout")
		default:
			reached := 0
			for i := 0; i < len(t.nodes); i++ {
				v1, _ := t.nodes[i].f3.Manifest.Manifest().Version()
				v2, _ := prev.Version()
				if v1 != v2 {
					reached++
				}
				if reached == len(t.nodes) {
					return
				}
			}
			time.Sleep(time.Second)
		}
	}
}

func newTestEnvironment(t *testing.T, n int, dynamicManifest bool) testEnv {
	env := testEnv{t: t}

	// populate manifest
	m := baseManifest
	m.ECBoostrapTimestamp = time.Now().Add(-time.Duration(m.BootstrapEpoch) * m.ECPeriod)

	env.signingBackend = signing.NewFakeBackend()
	for i := 0; i < n; i++ {
		pubkey, _ := env.signingBackend.GenerateKey()

		m.InitialPowerTable = append(m.InitialPowerTable, gpbft.PowerEntry{
			ID:     gpbft.ActorID(i),
			PubKey: pubkey,
			Power:  big.NewInt(1000),
		})
	}
	env.manifest = m
	env.ec = ec.NewFakeEC(1, m)

	manifestServer := peer.ID("")
	if dynamicManifest {
		env.newManifestSender(context.Background())
		manifestServer = env.manifestSender.SenderID()
	}

	// initialize nodes
	for i := 0; i < n; i++ {
		n, err := env.newF3Instance(context.Background(), i, manifestServer)
		require.NoError(t, err)
		env.nodes = append(env.nodes, n)
	}
	return env
}

func (e *testEnv) requireEqualManifests() {
	m := e.nodes[0].f3.Manifest
	for _, n := range e.nodes {
		require.Equal(e.t, n.f3.Manifest.Manifest(), m.Manifest())
	}
}

func (e *testEnv) Run(ctx context.Context, initialInstance uint64) {
	// Start the nodes
	for _, n := range e.nodes {
		go func(n *testNode) {
			// TODO: Handle error from Run
			_ = n.f3.Run(initialInstance, ctx)
		}(n)
	}

	// If it exists, start the manifest sender
	if e.manifestSender != nil {
		go func() {
			e.manifestSender.Start(ctx)
		}()
	}
}

func (e *testEnv) Connect(ctx context.Context) {
	for i, n := range e.nodes {
		for j := i + 1; j < len(e.nodes); j++ {
			addr := e.nodes[j].h.Addrs()[0]
			pi, err := peer.AddrInfoFromString(fmt.Sprintf("%s/p2p/%s", addr.String(), e.nodes[j].h.ID()))
			require.NoError(e.t, err)
			err = n.h.Connect(ctx, *pi)
			require.NoError(e.t, err)
		}
	}

	// connect to the manifest server if it exists
	if e.manifestSender != nil {
		for _, n := range e.nodes {
			addr := e.manifestSender.Addrs()[0]
			pi, err := peer.AddrInfoFromString(fmt.Sprintf("%s/p2p/%s", addr.String(), e.manifestSender.SenderID()))
			require.NoError(e.t, err)
			err = n.h.Connect(ctx, *pi)
			require.NoError(e.t, err)
		}

	}
}

func (e *testEnv) newManifestSender(ctx context.Context) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/quic-v1"))
	require.NoError(e.t, err)

	ps, err := pubsub.NewGossipSub(ctx, h)
	require.NoError(e.t, err)

	e.manifestSender, err = passive.NewManifestSender(h, ps, &e.manifest, ManifestSenderTimeout)
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
		mprovider = passive.NewDynamicManifestProvider(e.manifest, ps, e.ec, manifestServer)
	} else {
		mprovider = manifest.NewStaticManifestProvider(e.manifest)
	}

	e.signingBackend.Allow(int(id))
	module, err := f3.New(ctx, gpbft.ActorID(id), mprovider, ds, h, manifestServer, ps, e.signingBackend, e.signingBackend, e.ec, log)
	if err != nil {
		return nil, xerrors.Errorf("creating module: %w", err)
	}

	mprovider.SetManifestChangeCallback(f3.ManifestChangeCallback(module))
	return &testNode{h, module}, nil
}
