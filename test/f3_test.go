package test

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/sim/signing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/failstore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	ManifestSenderTimeout = 1 * time.Second
	logLevel              = "info"
)

func TestF3Simple(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 2, false)

	env.connectAll()
	env.start()
	env.waitForInstanceNumber(5, 10*time.Second, false)
}

func TestF3PauseResumeCatchup(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 3, false)

	env.connectAll()
	env.start()
	env.waitForInstanceNumber(1, 30*time.Second, true)

	// Pausing two nodes should pause the network.
	env.pauseNode(1)
	env.pauseNode(2)

	env.clock.Add(1 * time.Second)
	oldInstance := env.nodes[0].currentGpbftInstance()
	env.clock.Add(1 * time.Second)
	newInstance := env.nodes[0].currentGpbftInstance()
	require.Equal(t, oldInstance, newInstance)

	// Resuming node 1 should continue agreeing on instances.
	env.resumeNode(1)
	require.Equal(t, oldInstance, newInstance)
	resumeInstance := newInstance + 1
	env.waitForInstanceNumber(resumeInstance, 30*time.Second, false)

	// Wait until we're far enough that pure GPBFT catchup should be impossible.
	targetInstance := resumeInstance + env.manifest.CommitteeLookback
	env.waitForInstanceNumber(targetInstance, 30*time.Second, false)

	pausedInstance := env.nodes[2].currentGpbftInstance()
	require.Less(t, pausedInstance, resumeInstance)

	env.resumeNode(2)

	// Everyone should catch up eventually
	env.waitForInstanceNumber(targetInstance, 30*time.Second, true)

	// Pause the "good" node.
	env.pauseNode(0)
	node0failInstance := env.nodes[0].currentGpbftInstance()

	// We should be able to make progress with the remaining nodes.
	env.waitForInstanceNumber(node0failInstance+3, 30*time.Second, false)
}

func TestF3FailRecover(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 2, false)

	// Make it possible to fail a single write for node 0.
	var failDsWrite atomic.Bool
	dsFailureFunc := func(op string) error {
		if failDsWrite.Load() {
			switch op {
			case "put", "batch-put":
				failDsWrite.Store(false)
				return fmt.Errorf("FAILURE!")
			}
		}
		return nil
	}

	env.injectDatastoreFailures(0, dsFailureFunc)

	env.connectAll()
	env.start()
	env.waitForInstanceNumber(1, 10*time.Second, true)

	// Inject a single write failure. This should prevent us from storing a single decision
	// decision.
	failDsWrite.Store(true)

	// We should proceed anyways (catching up via the certificate exchange protocol).
	oldInstance := env.nodes[0].currentGpbftInstance()
	env.waitForInstanceNumber(oldInstance+3, 10*time.Second, true)
}

func TestF3DynamicManifest_WithoutChanges(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()
	prev := env.nodes[0].f3.Manifest()

	env.waitForInstanceNumber(5, 10*time.Second, false)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)
}

func TestF3DynamicManifest_WithRebootstrap(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()

	prev := env.nodes[0].f3.Manifest()
	env.waitForInstanceNumber(3, 15*time.Second, false)
	prevInstance := env.nodes[0].currentGpbftInstance()

	env.manifest.BootstrapEpoch = 1253
	env.addParticipants(&env.manifest, []gpbft.ActorID{2, 3}, big.NewInt(1), false)
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

func TestF3DynamicManifest_WithPauseAndRebootstrap(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, 2, true)

	env.connectAll()
	env.start()

	prev := env.nodes[0].f3.Manifest()
	env.waitForInstanceNumber(10, 15*time.Second, false)
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
	env.clock.Add(1 * time.Minute)

	// check that it rebootstrapped and the number of instances is below prevInstance
	require.Less(t, env.nodes[0].currentGpbftInstance(), prevInstance)
	env.waitForInstanceNumber(3, 15*time.Second, false)
	require.NotEqual(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)
}

var base = manifest.Manifest{
	BootstrapEpoch:  950,
	InitialInstance: 0,
	NetworkName:     gpbft.NetworkName("f3-test/0"),
	GpbftConfig: manifest.GpbftConfig{
		Delta:                3 * time.Second,
		DeltaBackOffExponent: 2.,
		MaxLookaheadRounds:   5,
	},
	// EcConfig:        manifest.DefaultEcConfig,
	EcConfig: manifest.EcConfig{
		ECFinality:        10,
		CommitteeLookback: 5,
		// increased delay and period to accelerate test times.
		ECPeriod:                 30 * time.Second,
		ECDelayMultiplier:        1.0,
		BaseDecisionBackoffTable: []float64{1., 1.2},
	},
	CxConfig: manifest.DefaultCxConfig}

type testNode struct {
	e         *testEnv
	h         host.Host
	f3        *f3.F3
	dsErrFunc func(string) error
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
	clock          *clock.Mock

	manifest        manifest.Manifest
	manifestVersion uint64
}

// signals the update to the latest manifest in the environment.
func (e *testEnv) updateManifest() {
	m := e.manifest // copy because we mutate it locally.
	e.manifestVersion++
	nn := fmt.Sprintf("%s/%d", e.manifest.NetworkName, e.manifestVersion)
	m.NetworkName = gpbft.NetworkName(nn)
	e.manifestSender.UpdateManifest(&m)
}

func (e *testEnv) addParticipants(m *manifest.Manifest, participants []gpbft.ActorID, power *big.Int, runNodes bool) {
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
		m.ExplicitPower = append(m.ExplicitPower, gpbft.PowerEntry{
			ID:     gpbft.ActorID(nodeLen),
			PubKey: pubkey,
			Power:  power,
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

func (e *testEnv) waitForCondition(condition func() bool, timeout time.Duration) {
	e.t.Helper()
	start := time.Now()
	for {
		if condition() {
			break
		}
		e.advance()
		if time.Since(start) > timeout {
			e.t.Fatalf("test took too long")
		}
	}
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForInstanceNumber(instanceNumber uint64, timeout time.Duration, strict bool) {
	e.waitForCondition(func() bool {
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
	}, timeout)
}

func (e *testEnv) advance() {
	e.clock.Add(1 * time.Second)
}

func (e *testEnv) waitForManifestChange(prev *manifest.Manifest, timeout time.Duration) {
	e.t.Helper()
	e.waitForCondition(func() bool {
		for _, n := range e.nodes {
			if !n.f3.IsRunning() {
				continue
			}

			m := n.f3.Manifest()
			if m == nil {
				return false
			}

			if prev.Equal(m) {
				return false
			}
		}
		return true
	}, timeout)
}

func newTestEnvironment(t *testing.T, n int, dynamicManifest bool) *testEnv {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, clk := clock.WithMockClock(ctx)
	grp, ctx := errgroup.WithContext(ctx)
	env := &testEnv{t: t, errgrp: grp, testCtx: ctx, net: mocknet.New(), clock: clk}

	// Cleanup on exit.
	env.t.Cleanup(func() {
		cancel()
		for _, n := range env.nodes {
			require.NoError(env.t, n.f3.Stop(context.Background()))
		}
		env.clock.Add(500 * time.Second)
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
	env.ec = ec.NewFakeEC(ctx, 1, m.BootstrapEpoch+m.ECFinality, m.ECPeriod, initialPowerTable, true)

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
	e.waitForCondition(func() bool {
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
	}, timeout)
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
}

func (e *testEnv) pauseNode(i int) {
	n := e.nodes[i]
	require.NoError(e.t, n.f3.Pause())
}

func (e *testEnv) resumeNode(i int) {
	n := e.nodes[i]
	require.NoError(e.t, n.f3.Resume())
}

func (e *testEnv) startNode(i int) {
	n := e.nodes[i]
	require.NoError(e.t, n.f3.Start(e.testCtx))
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
	e.manifestSender, err = manifest.NewManifestSender(e.testCtx, h, ps, &m, ManifestSenderTimeout)
	require.NoError(e.t, err)
}

func (e *testEnv) newF3Instance(id int, manifestServer peer.ID) (*testNode, error) {
	h, err := e.net.GenPeer()
	if err != nil {
		return nil, fmt.Errorf("creating libp2p host: %w", err)
	}

	n := &testNode{e: e, h: h}

	ps, err := pubsub.NewGossipSub(e.testCtx, h)
	if err != nil {
		return nil, fmt.Errorf("creating gossipsub: %w", err)
	}

	ds := ds_sync.MutexWrap(failstore.NewFailstore(datastore.NewMapDatastore(), func(s string) error {
		if n.dsErrFunc != nil {
			return (n.dsErrFunc)(s)
		}
		return nil
	}))

	m := e.manifest // copy because we mutate this
	var mprovider manifest.ManifestProvider
	if manifestServer != peer.ID("") {
		mprovider = manifest.NewDynamicManifestProvider(&m, ps, manifestServer)
	} else {
		mprovider = manifest.NewStaticManifestProvider(&m)
	}

	e.signingBackend.Allow(int(id))

	n.f3, err = f3.New(e.testCtx, mprovider, ds, h, ps, e.signingBackend, e.ec)
	if err != nil {
		return nil, fmt.Errorf("creating module: %w", err)
	}

	e.errgrp.Go(func() error {
		return runMessageSubscription(e.testCtx, n.f3, gpbft.ActorID(id), e.signingBackend)
	})

	return n, nil
}

func (e *testEnv) injectDatastoreFailures(i int, fn func(op string) error) {
	e.nodes[i].dsErrFunc = fn
}

// TODO: This code is copy-pasta from cmd/f3/run.go, consider taking it out into a shared testing lib.
// We could do the same to the F3 test instantiation
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
