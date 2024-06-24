//go:build !race

// Note: We don't run these tests with race because UnsafeCurrentInstance
// would trigger the race detector.
package test

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/certs"
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

	env.Connect(ctx)
	env.Run(ctx)
	go env.monitorHostErrs()
	// Small wait for nodes to initialize. In the future we can probably
	// make this asynchronously
	time.Sleep(10000000 * time.Second)
	env.waitForInstanceNumber(ctx, 5, 10000000*time.Second, false)
}

func TestDynamicManifest_WithoutChanges(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.Run(ctx)
	go env.monitorHostErrs()
	prev := env.nodes[0].f3.Manifest.Manifest()
	time.Sleep(1 * time.Second)

	env.waitForInstanceNumber(ctx, 5, 10*time.Second, false)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
}

func TestDynamicManifest_WithRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.Run(ctx)
	go env.monitorHostErrs()
	time.Sleep(1 * time.Second)

	prev := env.nodes[0].f3.Manifest.Manifest()

	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
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
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
}

func TestDynamicManifest_SubsequentWithRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.Run(ctx)
	go env.monitorHostErrs()
	time.Sleep(1 * time.Second)

	prev := env.nodes[0].f3.Manifest.Manifest()

	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
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
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

	// New manifest with sequence 2
	prev = env.nodes[0].f3.Manifest.Manifest()
	prevInstance = env.nodes[0].f3.CurrentGpbftInstace()

	env.manifest.BootstrapEpoch = 960
	env.manifest.Sequence = 2
	env.manifest.ReBootstrap = true
	env.manifestSender.UpdateManifest(&env.manifest)

	env.waitForManifestChange(ctx, prev, 15*time.Second)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.True(t, env.nodes[0].f3.CurrentGpbftInstace() < prevInstance)
	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

	// Using a manifest with a lower sequence number doesn't trigger an update
	prev = env.nodes[0].f3.Manifest.Manifest()
	env.manifest.BootstrapEpoch = 965
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = true
	env.manifestSender.UpdateManifest(&env.manifest)

	// wait for the new manifest to be broadcast
	time.Sleep(5 * ManifestSenderTimeout)

	// check that no manifest change has propagated
	require.Equal(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)

}

func TestDynamicManifest_WithoutRebootstrap(t *testing.T) {
	ctx := context.Background()
	env := newTestEnvironment(t, 2, true)

	env.Connect(ctx)
	env.Run(ctx)
	go env.monitorHostErrs()
	time.Sleep(1 * time.Second)

	prev := env.nodes[0].f3.Manifest.Manifest()

	env.waitForInstanceNumber(ctx, 3, 15*time.Second, false)
	prevInstance := env.nodes[0].f3.CurrentGpbftInstace()

	env.manifest.BootstrapEpoch = 953
	env.manifest.Sequence = 1
	env.manifest.ReBootstrap = false
	// Adding a power of 1 so we can reach consensus without having to run
	// the new nodes
	env.addPowerDeltaForParticipants(ctx, &env.manifest, []gpbft.ActorID{2, 3}, big.NewInt(1), false)
	env.manifestSender.UpdateManifest(&env.manifest)

	env.waitForManifestChange(ctx, prev, 15*time.Second)

	// check that the runner continued without rebootstrap
	require.True(t, env.nodes[0].f3.CurrentGpbftInstace() >= prevInstance)
	env.waitForInstanceNumber(ctx, prevInstance+7, 150*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest.Manifest())
	env.requireEqualManifests(false)
	// check that the power table is updated with the new entries
	ts, err := env.ec.GetTipsetByEpoch(ctx, int64(env.nodes[0].f3.CurrentGpbftInstace()))
	require.NoError(t, err)
	pt, err := env.nodes[0].f3.GetPowerTable(ctx, ts.Key())
	require.NoError(t, err)
	require.Equal(t, len(pt), 4)
}

var baseManifest manifest.Manifest = manifest.Manifest{
	Sequence:        0,
	BootstrapEpoch:  950,
	ReBootstrap:     true,
	InitialInstance: 0,
	NetworkName:     gpbft.NetworkName("f3-test"),
	GpbftConfig:     manifest.DefaultGpbftConfig,
	// EcConfig:       manifest.DefaultEcConfig,
	EcConfig: &manifest.EcConfig{
		ECFinality:       10,
		CommiteeLookback: 5,
		ECDelay:          500 * time.Millisecond,
		// Increase the period to reduce the test time
		ECPeriod: 500 * time.Millisecond,
	},
}

type testNode struct {
	h     host.Host
	f3    *f3.F3
	errCh <-chan error
}

type testEnv struct {
	t              *testing.T
	manifest       manifest.Manifest
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *ec.FakeEC

	manifestSender *passive.ManifestSender
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
			_ = e.nodes[nodeLen-1].f3.Run(context.Background())
		}
	}
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForInstanceNumber(ctx context.Context, instanceNumber uint64, timeout time.Duration, strict bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			e.t.Fatal("instance number not reached before timeout")
		default:
			reached := 0
			for i := 0; i < len(e.nodes); i++ {
				// nodes that are not running are not required to reach the instance
				// (it will actually panic if we try to fetch it because there is no
				// runner initialized)
				if !e.nodes[i].f3.IsRunning() && !strict {
					reached++
				} else if e.nodes[i].f3.CurrentGpbftInstace() >= instanceNumber && e.nodes[i].f3.IsRunning() {
					reached++
				}
				if reached >= len(e.nodes) {
					return
				}
			}
			time.Sleep(time.Second)
		}
	}
}

func (e *testEnv) waitForManifestChange(ctx context.Context, prev manifest.Manifest, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			e.t.Fatal("manifest change not reached before timeout")
		default:
			reached := 0
			for i := 0; i < len(e.nodes); i++ {
				// only running nodes will be listening to manifest changes
				if !e.nodes[i].f3.IsRunning() {
					reached++
				} else {
					v1, _ := e.nodes[i].f3.Manifest.Manifest().Version()
					v2, _ := prev.Version()
					if v1 != v2 {
						reached++
					}
				}
				if reached == len(e.nodes) {
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

func (e *testEnv) Run(ctx context.Context) {
	// Start the nodes
	for _, n := range e.nodes {
		go func(n *testNode) {
			errCh := make(chan error)
			n.errCh = errCh
			err := n.f3.Run(ctx)
			errCh <- err
		}(n)
	}

	// If it exists, start the manifest sender
	if e.manifestSender != nil {
		go func() {
			e.manifestSender.Start(ctx)
		}()
	}
}

func (e *testEnv) monitorHostErrs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	for _, n := range e.nodes {
		wg.Add(1)
		go func(n *testNode) {
			defer wg.Done()
			select {
			case err := <-n.errCh:
				if err != nil {
					cancel()
					require.NoError(e.t, err)
					return
				}
			case <-ctx.Done():
				return
			}
		}(n)
	}

	// Wait for all goroutines to finish
	wg.Wait()
}

func (e *testEnv) connectNodes(ctx context.Context, n1, n2 *testNode) {
	addr := n2.h.Addrs()[0]
	pi, err := peer.AddrInfoFromString(fmt.Sprintf("%s/p2p/%s", addr.String(), n2.h.ID()))
	require.NoError(e.t, err)
	err = n1.h.Connect(ctx, *pi)
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
