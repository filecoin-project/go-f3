package f3_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/filecoin-project/go-f3/internal/psutil"
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

func init() {
	// Hash-based deduplication breaks fast rebroadcast, even if we set the time window to be
	// really short because gossipsub has a minimum 1m cache scan interval.
	psutil.GPBFTMessageIdFn = pubsub.DefaultMsgIdFn
	psutil.ManifestMessageIdFn = pubsub.DefaultMsgIdFn
}

var manifestSenderTimeout = 10 * time.Second

func TestF3Simple(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2).start()
	env.waitForInstanceNumber(5, 10*time.Second, false)
}

func TestF3WithLookback(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2).withManifest(func(m *manifest.Manifest) {
		m.EC.HeadLookback = 20
	}).start()

	env.waitForInstanceNumber(5, 10*time.Second, false)

	// Wait a second to let everything settle.
	time.Sleep(10 * time.Millisecond)

	headEpoch := env.ec.GetCurrentHead()

	cert, err := env.nodes[0].f3.GetLatestCert(env.testCtx)
	require.NoError(t, err)
	require.NotNil(t, cert)

	// just in case we race, I'm using 15 not 20 here.
	require.LessOrEqual(t, cert.ECChain.Head().Epoch, headEpoch-15)

	env.ec.Pause()

	// Advance by 100 periods.
	for i := 0; i < 200; i++ {
		env.clock.Add(env.manifest.EC.Period / 2)
		time.Sleep(time.Millisecond)
	}

	// Now make sure we've advanced by less than 100 instances. We want to make sure we're not
	// racing.
	cert, err = env.nodes[0].f3.GetLatestCert(env.testCtx)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Less(t, cert.GPBFTInstance, uint64(90))

	// If we add another EC period, we should make progress again.
	// We do it bit by bit to give code time to run.
	env.ec.Resume()
	for i := 0; i < 10; i++ {
		env.clock.Add(env.manifest.EC.Period / 10)
		time.Sleep(time.Millisecond)
	}

	env.waitForInstanceNumber(5, 10*time.Second, true)
}

func TestF3PauseResumeCatchup(t *testing.T) {
	env := newTestEnvironment(t).withNodes(3).start()

	env.waitForInstanceNumber(1, 30*time.Second, true)

	// Pausing two nodes should pause the network.
	env.pauseNode(1)
	env.pauseNode(2)

	// Wait until node 0 stops receiving new instances
	env.clock.Add(1 * time.Second)
	env.waitForCondition(func() bool {
		oldInstance := env.nodes[0].currentGpbftInstance()
		env.clock.Add(10 * env.manifest.EC.Period)
		newInstance := env.nodes[0].currentGpbftInstance()
		return oldInstance == newInstance
	}, 30*time.Second)

	// Resuming node 1 should continue agreeing on instances.
	env.resumeNode(1)

	// Wait until we're far enough that pure GPBFT catchup should be impossible.
	targetInstance := env.nodes[1].currentGpbftInstance() + env.manifest.CommitteeLookback + 1
	env.waitForInstanceNumber(targetInstance, 60*time.Second, false)

	env.resumeNode(2)

	// Everyone should catch up eventually
	env.waitForInstanceNumber(targetInstance, 30*time.Second, true)

	// Pause the "good" node.
	node0failInstance := env.nodes[0].currentGpbftInstance()
	env.pauseNode(0)

	// We should be able to make progress with the remaining nodes.
	env.waitForInstanceNumber(node0failInstance+3, 60*time.Second, false)
}

func TestF3FailRecover(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2)

	// Make it possible to fail a single write for node 0.
	var failDsWrite atomic.Bool
	dsFailureFunc := func(op string) error {
		if failDsWrite.Load() {
			switch op {
			case "put", "batch-put":
				failDsWrite.Store(false)
				return fmt.Errorf("intentional error for testing, please ignore")
			}
		}
		return nil
	}

	env.injectDatastoreFailures(0, dsFailureFunc)

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
	env := newTestEnvironment(t).withNodes(2).withDynamicManifest()

	env.start()
	prev := env.nodes[0].f3.Manifest()

	env.waitForInstanceNumber(5, 10*time.Second, false)
	// no changes in manifest
	require.Equal(t, prev, env.nodes[0].f3.Manifest())
	env.requireEqualManifests(false)
}

func TestF3DynamicManifest_WithRebootstrap(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2).withDynamicManifest().start()

	prev := env.nodes[0].f3.Manifest()
	env.waitForInstanceNumber(3, 15*time.Second, true)

	env.manifest.BootstrapEpoch = 1253
	for i := 0; i < 2; i++ {
		nd := env.addNode()
		pubkey, _ := env.signingBackend.GenerateKey()
		env.manifest.ExplicitPower = append(env.manifest.ExplicitPower, gpbft.PowerEntry{
			ID:     gpbft.ActorID(nd.id),
			PubKey: pubkey,
			Power:  gpbft.NewStoragePower(1),
		})
	}

	env.updateManifest()
	env.waitForManifest()

	// check that it rebootstrapped and has a new base epoch.
	targetBaseEpoch := env.manifest.BootstrapEpoch - env.manifest.EC.Finality
	env.waitForCondition(func() bool {
		env.clock.Add(env.manifest.EC.Period)
		c, err := env.nodes[0].f3.GetCert(env.testCtx, 0)
		if err != nil || c == nil {
			return false
		}
		return c.ECChain.Base().Epoch == targetBaseEpoch
	}, 20*time.Second)
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
	env := newTestEnvironment(t).withNodes(2).withDynamicManifest().start()

	env.waitForInstanceNumber(10, 30*time.Second, true)

	env.manifest.Pause = true
	env.updateManifest()

	// check that it paused
	env.waitForNodesStopped()

	env.manifest.BootstrapEpoch = 956
	env.manifest.Pause = false
	env.updateManifest()
	env.waitForManifest()

	env.clock.Add(1 * time.Minute)

	env.waitForInstanceNumber(3, 30*time.Second, true)
	env.requireEqualManifests(true)

	// Now check that we have the correct base for certificate 0.
	cert0, err := env.nodes[0].f3.GetCert(env.testCtx, 0)
	require.NoError(t, err)
	require.Equal(t, env.manifest.BootstrapEpoch-env.manifest.EC.Finality, cert0.ECChain.Base().Epoch)
}

func TestF3DynamicManifest_RebootstrapWithCompression(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2).withDynamicManifest().start()
	env.waitForInstanceNumber(10, 30*time.Second, true)

	env.manifest.Pause = true
	env.updateManifest()

	env.waitForNodesStopped()

	env.manifest.BootstrapEpoch = 956
	env.manifest.PubSub.CompressionEnabled = true
	env.manifest.Pause = false
	env.updateManifest()
	env.waitForManifest()

	env.clock.Add(1 * time.Minute)

	env.waitForInstanceNumber(3, 30*time.Second, true)
	env.requireEqualManifests(true)

	cert0, err := env.nodes[0].f3.GetCert(env.testCtx, 0)
	require.NoError(t, err)
	require.Equal(t, env.manifest.BootstrapEpoch-env.manifest.EC.Finality, cert0.ECChain.Base().Epoch)
}

func TestF3LateBootstrap(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2).start()

	// Wait till we're "caught up".
	bootstrapInstances := uint64(env.manifest.EC.Finality/(gpbft.ChainDefaultLen-1)) + 1
	env.waitForInstanceNumber(bootstrapInstances, 30*time.Second, true)

	// Wait until we've finalized a distant epoch. Once we do, our EC will forget the historical
	// chain (importantly, forget the bootstrap power table).
	targetEpoch := 2*env.manifest.EC.Finality + env.manifest.BootstrapEpoch
	env.waitForEpochFinalized(targetEpoch)

	// Now update the manifest with the initial power-table CID.
	cert0, err := env.nodes[0].f3.GetCert(env.testCtx, 0)
	require.NoError(t, err)
	env.manifest.InitialPowerTable = cert0.ECChain.Base().PowerTable

	// Create/start a new node.
	f3 := env.addNode().init()
	env.connectAll()
	// We start async because we need to drive the clock forward while fetching the initial
	// power table.
	env.errgrp.Go(func() error {
		return f3.Start(env.testCtx)
	})

	// Wait for it to finish starting.
	env.waitForCondition(func() bool {
		env.clock.Add(env.manifest.EC.Period)
		// This step takes time, give it time.
		time.Sleep(10 * time.Millisecond)
		return f3.IsRunning()
	}, 10*time.Second)

	// It should eventually catch up.
	env.waitForCondition(func() bool {
		cert, err := f3.GetLatestCert(env.testCtx)
		require.NoError(t, err)
		return cert != nil && cert.ECChain.Head().Epoch > targetEpoch
	}, 10*time.Second)
}

var base = manifest.Manifest{
	BootstrapEpoch:      950,
	InitialInstance:     0,
	NetworkName:         gpbft.NetworkName("f3-test"),
	CommitteeLookback:   manifest.DefaultCommitteeLookback,
	Gpbft:               manifest.DefaultGpbftConfig,
	EC:                  manifest.DefaultEcConfig,
	CertificateExchange: manifest.DefaultCxConfig,
	CatchUpAlignment:    manifest.DefaultCatchUpAlignment,
	PubSub:              manifest.DefaultPubSubConfig,
}

type testNode struct {
	e         *testEnv
	h         host.Host
	id        int
	f3        *f3.F3
	dsErrFunc func(string) error
}

func (n *testNode) currentGpbftInstance() uint64 {
	c, err := n.f3.GetLatestCert(n.e.testCtx)
	require.NoError(n.e.t, err)
	if c == nil {
		return n.e.manifest.InitialInstance
	}
	return c.GPBFTInstance + 1
}

func (n *testNode) init() *f3.F3 {
	if n.f3 != nil {
		return n.f3
	}

	// We disable message signing in tests to make things faster.
	ps, err := pubsub.NewGossipSub(n.e.testCtx, n.h, pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign))
	require.NoError(n.e.t, err)

	ds := ds_sync.MutexWrap(failstore.NewFailstore(datastore.NewMapDatastore(), func(s string) error {
		if n.dsErrFunc != nil {
			return (n.dsErrFunc)(s)
		}
		return nil
	}))

	var manifestServerID peer.ID
	if n.e.manifestSender != nil {
		manifestServerID = n.e.manifestSender.SenderID()
	}

	var mprovider manifest.ManifestProvider
	if manifestServerID != "" {
		mprovider, err = manifest.NewDynamicManifestProvider(
			ps, manifestServerID,
			manifest.DynamicManifestProviderWithInitialManifest(n.e.currentManifest()),
		)
	} else {
		mprovider, err = manifest.NewStaticManifestProvider(n.e.currentManifest())
	}
	require.NoError(n.e.t, err)

	n.e.signingBackend.Allow(int(n.id))

	n.f3, err = f3.New(n.e.testCtx, mprovider, ds, n.h, ps, n.e.signingBackend, n.e.ec,
		filepath.Join(n.e.tempDir, fmt.Sprintf("instance-%d", n.id)))
	require.NoError(n.e.t, err)

	n.e.errgrp.Go(func() error {
		return runMessageSubscription(n.e.testCtx, n.f3, gpbft.ActorID(n.id), n.e.signingBackend)
	})

	return n.f3
}

func (n *testNode) pause() {
	require.NoError(n.e.t, n.f3.Pause(n.e.testCtx))
}

func (n *testNode) resume() {
	require.NoError(n.e.t, n.f3.Resume(n.e.testCtx))
}

type testEnv struct {
	t              *testing.T
	errgrp         *errgroup.Group
	testCtx        context.Context
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *consensus.FakeEC
	manifestSender *manifest.ManifestSender
	net            mocknet.Mocknet
	clock          *clock.Mock
	tempDir        string // we need to ask for it before any of our cleanup hooks

	manifest        manifest.Manifest
	manifestVersion uint64
}

func (e *testEnv) currentManifest() *manifest.Manifest {
	m := e.manifest
	if e.manifestSender != nil {
		nn := fmt.Sprintf("%s/%d", e.manifest.NetworkName, e.manifestVersion)
		m.NetworkName = gpbft.NetworkName(nn)
	}
	return &m
}

// signals the update to the latest manifest in the environment.
func (e *testEnv) updateManifest() {
	e.t.Helper()
	if e.manifestSender == nil {
		e.t.Fatal("cannot update manifest unless the dynamic manifest is enabled")
	}
	e.manifestVersion++
	e.manifestSender.UpdateManifest(e.currentManifest())
}

func (e *testEnv) waitForManifest() {
	e.t.Helper()
	newManifest := e.currentManifest()
	e.waitForCondition(func() bool {
		for _, n := range e.nodes {
			if n.f3 == nil {
				continue
			}

			m := n.f3.Manifest()
			if m == nil {
				return false
			}
			if !newManifest.Equal(m) {
				return false
			}
		}
		return true
	}, 60*time.Second)
}

func (e *testEnv) waitForCondition(condition func() bool, timeout time.Duration) {
	e.t.Helper()
	start := time.Now()
	for !condition() {
		if time.Since(start) > timeout {
			e.t.Fatalf("test took too long (more than %s)", timeout)
		}
		e.advance()
	}
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForInstanceNumber(instanceNumber uint64, timeout time.Duration, strict bool) {
	e.t.Helper()
	e.waitForCondition(func() bool {
		for _, n := range e.nodes {
			// nodes that are not running are not required to reach the instance
			// (it will actually panic if we try to fetch it because there is no
			// runner initialized)
			if n.f3 == nil || !n.f3.IsRunning() {
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

func (e *testEnv) withNodes(n int) *testEnv {
	for i := 0; i < n; i++ {
		e.addNode()
	}

	return e
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) waitForEpochFinalized(epoch int64) {
	e.t.Helper()
	for {
		head := e.ec.GetCurrentHead()
		if head > e.manifest.BootstrapEpoch {
			e.waitForCondition(func() bool {
				time.Sleep(time.Millisecond)
				for _, nd := range e.nodes {
					if nd.f3 == nil || !nd.f3.IsRunning() {
						continue
					}
					cert, err := nd.f3.GetLatestCert(e.testCtx)
					if cert == nil || err != nil {
						continue
					}
					if cert.ECChain.Head().Epoch >= head {
						return true
					}
				}
				return false
			}, 30*time.Second)
		}

		if head < epoch-100 {
			e.clock.Add(100 * e.manifest.EC.Period)
		} else if head < epoch-10 {
			e.clock.Add(10 * e.manifest.EC.Period)
		} else if head < epoch {
			e.clock.Add(1 * e.manifest.EC.Period)
		} else {
			break
		}
	}
}

func newTestEnvironment(t *testing.T) *testEnv {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, clk := clock.WithMockClock(ctx)
	grp, ctx := errgroup.WithContext(ctx)
	env := &testEnv{
		t:              t,
		errgrp:         grp,
		testCtx:        ctx,
		net:            mocknet.New(),
		clock:          clk,
		tempDir:        t.TempDir(),
		manifest:       base,
		signingBackend: signing.NewFakeBackend(),
	}

	// Cleanup on exit.
	env.t.Cleanup(func() {
		require.NoError(env.t, env.net.Close())

		cancel()
		for _, n := range env.nodes {
			if n.f3 != nil {
				require.NoError(env.t, n.f3.Stop(context.Background()))
			}
		}
		env.clock.Add(500 * time.Second)
		require.NoError(env.t, env.errgrp.Wait())
	})

	return env
}

func (e *testEnv) addNode() *testNode {
	h, err := e.net.GenPeer()
	require.NoError(e.t, err)
	n := &testNode{e: e, h: h, id: len(e.nodes)}
	e.nodes = append(e.nodes, n)
	return n
}

func (e *testEnv) requireEqualManifests(strict bool) {
	m := e.nodes[0].f3.Manifest()
	for _, n := range e.nodes {
		// only check running nodes
		if (n.f3 != nil && n.f3.IsRunning()) || strict {
			require.Equal(e.t, n.f3.Manifest(), m)
		}
	}
}

func (e *testEnv) waitFor(f func(n *testNode) bool, timeout time.Duration) {
	e.t.Helper()
	e.waitForCondition(func() bool {
		for _, n := range e.nodes {
			if !f(n) {
				return false
			}
		}
		return true
	}, timeout)
}

func (e *testEnv) waitForNodesInitialization() {
	e.t.Helper()
	e.waitFor(func(n *testNode) bool {
		return n.f3.IsRunning()
	}, 30*time.Second)
}

func (e *testEnv) waitForNodesStopped() {
	e.t.Helper()
	e.waitFor(func(n *testNode) bool {
		return !n.f3.IsRunning()
	}, 30*time.Second)
}

// Connect & link all nodes. This operation is idempotent.
func (e *testEnv) connectAll() *testEnv {
	require.NoError(e.t, e.net.LinkAll())
	require.NoError(e.t, e.net.ConnectAllButSelf())
	return e
}

// Initialize all nodes and create the initial power table. This operation can be called multiple
// times, but nodes added after it is called won't be added to the power table.
func (e *testEnv) initialize() *testEnv {
	// Construct the EC if necessary.
	if e.ec == nil {
		initialPowerTable := gpbft.PowerEntries{}
		for _, n := range e.nodes {
			pubkey, _ := e.signingBackend.GenerateKey()
			initialPowerTable = append(initialPowerTable, gpbft.PowerEntry{
				ID:     gpbft.ActorID(n.id),
				PubKey: pubkey,
				Power:  gpbft.NewStoragePower(1000),
			})
		}

		e.ec = consensus.NewFakeEC(e.testCtx,
			consensus.WithBootstrapEpoch(e.manifest.BootstrapEpoch),
			consensus.WithMaxLookback(2*e.manifest.EC.Finality),
			consensus.WithECPeriod(e.manifest.EC.Period),
			consensus.WithInitialPowerTable(initialPowerTable),
		)
	}

	for _, n := range e.nodes {
		n.init()
	}
	return e
}

// Start all nodes. This will also connect/initialize all nodes if necessary.
func (e *testEnv) start() *testEnv {
	e.t.Helper()
	e.initialize()
	e.connectAll()

	// Start the nodes. Async because this can block and we need time to progress.
	for _, n := range e.nodes {
		e.errgrp.Go(func() error {
			return n.init().Start(n.e.testCtx)
		})
	}

	// wait for nodes to initialize
	e.waitForNodesInitialization()

	// If it exists, start the manifest sender
	if e.manifestSender != nil {
		e.errgrp.Go(func() error { return e.manifestSender.Run(e.testCtx) })
	}

	return e
}

func (e *testEnv) withManifest(fn func(m *manifest.Manifest)) *testEnv {
	fn(&e.manifest)
	return e
}

func (e *testEnv) pauseNode(i int) {
	e.nodes[i].pause()
}

func (e *testEnv) resumeNode(i int) {
	e.nodes[i].resume()
}

func (e *testEnv) withDynamicManifest() *testEnv {
	h, err := e.net.GenPeer()
	require.NoError(e.t, err)

	ps, err := pubsub.NewGossipSub(e.testCtx, h, pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign))
	require.NoError(e.t, err)

	e.manifestSender, err = manifest.NewManifestSender(e.testCtx, h, ps,
		e.currentManifest(), manifestSenderTimeout)
	require.NoError(e.t, err)

	return e
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
				// This isn't an error, it just means we have no power.
				continue
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
