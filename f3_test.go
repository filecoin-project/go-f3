package f3_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/ec"
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
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	eventualCheckInterval = 100 * time.Millisecond
	eventualCheckTimeout  = time.Minute
	advanceClockEvery     = 5 * time.Millisecond
	advanceClockBy        = 100 * time.Millisecond
)

func init() {
	// Hash-based deduplication breaks fast rebroadcast, even if we set the time window to be
	// really short because gossipsub has a minimum 1m cache scan interval.
	psutil.GPBFTMessageIdFn = pubsub.DefaultMsgIdFn
	psutil.ManifestMessageIdFn = pubsub.DefaultMsgIdFn
	psutil.ChainExchangeMessageIdFn = pubsub.DefaultMsgIdFn
}

func TestF3Simple(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t).withNodes(2).start()
	env.requireInstanceEventually(5, eventualCheckTimeout, true)
	env.requireEpochFinalizedEventually(env.manifest.BootstrapEpoch, eventualCheckTimeout)
}

func TestF3WithLookback(t *testing.T) {
	t.Parallel()

	mfst := base
	mfst.EC.HeadLookback = 20

	env := newTestEnvironment(t).
		withNodes(2).
		withManifest(mfst).
		start()
	env.requireInstanceEventually(5, eventualCheckTimeout, true)

	headEpoch := env.ec.GetCurrentHead()
	cert, err := env.nodes[0].f3.GetLatestCert(env.testCtx)
	require.NoError(t, err)
	require.NotNil(t, cert)

	// just in case we race, I'm using 15 not 20 here.
	require.LessOrEqual(t, cert.ECChain.Head().Epoch, headEpoch-15)

	env.ec.Pause()

	// Advance by 100 periods.
	env.clock.Add(env.manifest.EC.Period * 100)

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
	}

	env.requireInstanceEventually(5, eventualCheckTimeout, true)
}

func TestF3PauseResumeCatchup(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t).withNodes(3).start()
	env.requireInstanceEventually(1, eventualCheckTimeout, true)
	env.requireEpochFinalizedEventually(env.manifest.BootstrapEpoch, eventualCheckTimeout)

	// Pausing two nodes should pause the network.
	env.pauseNode(1)
	env.pauseNode(2)

	env.requireF3NotRunningEventually(eventualCheckTimeout, nodeMatchers.byID(1, 2))

	// Wait until node 0 stops progressing
	require.Eventually(t, func() bool {
		before := env.nodes[0].status()
		env.clock.Add(env.manifest.EC.Period)
		after := env.nodes[0].status()
		return before == after
	}, eventualCheckTimeout, eventualCheckInterval)

	// Resuming node 1 should continue agreeing on instances.
	env.resumeNode(1)
	env.requireF3RunningEventually(eventualCheckTimeout, nodeMatchers.byID(1))

	// Wait until we're far enough that pure GPBFT catchup should be impossible.
	targetInstance := env.nodes[1].currentGpbftInstance() + env.manifest.CommitteeLookback + 1
	env.requireInstanceEventually(targetInstance, eventualCheckTimeout, false)

	env.resumeNode(2)
	env.requireF3RunningEventually(eventualCheckTimeout, nodeMatchers.byID(2))

	// Everyone should catch up eventually
	env.requireInstanceEventually(targetInstance, eventualCheckTimeout, true)

	// Pause the "good" node.
	node0failInstance := env.nodes[0].currentGpbftInstance()
	env.pauseNode(0)
	env.requireF3NotRunningEventually(eventualCheckTimeout, nodeMatchers.byID(0))

	// We should be able to make progress with the remaining nodes.
	env.requireInstanceEventually(node0failInstance+3, eventualCheckTimeout, false)
}

func TestF3FailRecover(t *testing.T) {
	t.Parallel()
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
	env.requireInstanceEventually(1, eventualCheckTimeout, true)

	// Inject a single write failure. This should prevent us from storing a single decision
	// decision.
	failDsWrite.Store(true)

	// We should proceed anyways (catching up via the certificate exchange protocol).
	oldInstance := env.nodes[0].currentGpbftInstance()
	env.requireInstanceEventually(oldInstance+3, eventualCheckTimeout, true)
}

func TestF3LateBootstrap(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t).withNodes(2).start()

	// Wait till we're "caught up".
	bootstrapInstances := uint64(env.manifest.EC.Finality/(gpbft.ChainDefaultLen-1)) + 1
	env.requireInstanceEventually(bootstrapInstances, eventualCheckTimeout, true)

	// Wait until we've finalized a distant epoch. Once we do, our EC will forget the historical
	// chain (importantly, forget the bootstrap power table).
	targetEpoch := 2*env.manifest.EC.Finality + env.manifest.BootstrapEpoch
	env.clock.Add(time.Duration(targetEpoch/3) * env.manifest.EC.Period) // Fast-forward to 1/3 of the target epoch for faster test e2e time.
	env.requireEpochFinalizedEventually(targetEpoch, eventualCheckTimeout)

	// Now update the manifest with the initial power-table CID.
	cert0, err := env.nodes[0].f3.GetCert(env.testCtx, 0)
	require.NoError(t, err)
	env.manifest.InitialPowerTable = cert0.ECChain.Base().PowerTable

	// Create/start a new node.
	newNode := env.addNode().init()
	env.connectAll()
	// We start async because we need to drive the clock forward while fetching the initial
	// power table.
	env.errgrp.Go(func() error {
		return newNode.Start(env.testCtx)
	})

	// Wait for it to finish starting.
	env.whileAdvancingClock(func() {
		require.Eventually(t, newNode.IsRunning, eventualCheckTimeout, eventualCheckInterval)
	})

	// It should eventually catch up.
	env.whileAdvancingClock(func() {
		require.Eventually(t, func() bool {
			cert, err := newNode.GetLatestCert(env.testCtx)
			require.NoError(t, err)
			return cert != nil && cert.ECChain.Head().Epoch > targetEpoch
		}, eventualCheckTimeout, eventualCheckInterval)
	})
}

func TestF3EpochFinalizedWithChainExchange(t *testing.T) {
	env := newTestEnvironment(t).withNodes(2)

	initialPowerTable := gpbft.PowerEntries{}
	for _, n := range env.nodes {
		pubkey, _ := env.signingBackend.GenerateKey()
		initialPowerTable = append(initialPowerTable, gpbft.PowerEntry{
			ID:     gpbft.ActorID(n.id),
			PubKey: pubkey,
			Power:  gpbft.NewStoragePower(1000),
		})
	}

	env.ec = consensus.NewFakeEC(
		consensus.WithClock(env.clock),
		consensus.WithSeed(1413),
		consensus.WithBootstrapEpoch(env.manifest.BootstrapEpoch),
		consensus.WithMaxLookback(2*env.manifest.EC.Finality),
		consensus.WithECPeriod(env.manifest.EC.Period),
		consensus.WithInitialPowerTable(initialPowerTable),
		consensus.WithForkSeed(1414),
		consensus.WithForkAfterEpochs(10),
	)

	env.nodes[0].ec = consensus.NewFakeEC(
		consensus.WithClock(env.clock),
		consensus.WithSeed(1413),
		consensus.WithBootstrapEpoch(env.manifest.BootstrapEpoch),
		consensus.WithMaxLookback(2*env.manifest.EC.Finality),
		consensus.WithECPeriod(env.manifest.EC.Period),
		consensus.WithInitialPowerTable(initialPowerTable),
		consensus.WithForkSeed(1415),
		consensus.WithForkAfterEpochs(3),
	)
	env.start()
	// Fast forward the clock to force nodes to catch up by generating longer chains.
	env.clock.Add(1 * time.Hour)
	env.requireEpochFinalizedEventually(env.manifest.BootstrapEpoch+10, eventualCheckTimeout)
}

var base = manifest.Manifest{
	BootstrapEpoch:    50,
	InitialInstance:   0,
	NetworkName:       gpbft.NetworkName("f3-test"),
	CommitteeLookback: manifest.DefaultCommitteeLookback,
	Gpbft: manifest.GpbftConfig{
		Delta:                      3 * time.Second,
		DeltaBackOffExponent:       1.3,
		QualityDeltaMultiplier:     1.0,
		MaxLookaheadRounds:         5,
		ChainProposedLength:        30,
		RebroadcastBackoffBase:     3 * time.Second,
		RebroadcastBackoffSpread:   0.1,
		RebroadcastBackoffExponent: 1.3,
		RebroadcastBackoffMax:      5 * time.Second,
	},
	EC: manifest.EcConfig{
		Finality:                 40,
		Period:                   10 * time.Second,
		DelayMultiplier:          1.3,
		BaseDecisionBackoffTable: []float64{1.3},
		HeadLookback:             4,
		Finalize:                 true,
	},
	CertificateExchange: manifest.DefaultCxConfig,
	CatchUpAlignment:    5 * time.Second,
	PubSub:              manifest.DefaultPubSubConfig,
	ChainExchange: manifest.ChainExchangeConfig{
		SubscriptionBufferSize:         32,
		MaxChainLength:                 30,
		MaxInstanceLookahead:           manifest.DefaultCommitteeLookback,
		MaxDiscoveredChainsPerInstance: 1_000,
		MaxWantedChainsPerInstance:     1_000,
		RebroadcastInterval:            2 * time.Second,
		MaxTimestampAge:                8 * time.Second,
	},
	PartialMessageManager: manifest.DefaultPartialMessageManagerConfig,
}

type testNode struct {
	e         *testEnv
	h         host.Host
	id        int
	f3        *f3.F3
	dsErrFunc func(string) error
	ec        ec.Backend
}

func (n *testNode) currentGpbftInstance() uint64 {
	require.NotNil(n.e.t, n.f3)
	return n.f3.Progress().ID
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

	n.e.signingBackend.Allow(n.id)

	if n.ec == nil {
		n.ec = n.e.ec
	}
	n.f3, err = f3.New(n.e.testCtx, n.e.manifest, ds, n.h, ps, n.e.signingBackend, n.ec,
		filepath.Join(n.e.tempDir, fmt.Sprintf("participant-%d", n.id)))
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

type testNodeStatus struct {
	id          int
	initialised bool
	running     bool
	progress    gpbft.InstanceProgress
	latestCert  *certs.FinalityCertificate
}

func (s testNodeStatus) String() string {
	switch {
	case !s.initialised:
		return fmt.Sprint("node ", s.id, ": uninitialised")
	case !s.running:
		return fmt.Sprint("node ", s.id, ": paused")
	case s.latestCert == nil:
		return fmt.Sprint("node ", s.id, ": progress ", s.progress, ", with no finality certs")
	default:
		return fmt.Sprint("node ", s.id, ": progress ", s.progress, ", finalized epoch ", s.latestCert.ECChain.Head().Epoch, " at instance ", s.latestCert.GPBFTInstance)
	}
}

func (n *testNode) status() testNodeStatus {
	var current testNodeStatus
	current.id = n.id
	if n.f3 != nil {
		current.initialised = true
		current.running = n.f3.IsRunning()
		current.progress = n.f3.Progress()
		if current.running {
			cert, err := n.f3.GetLatestCert(n.e.testCtx)
			require.NoError(n.e.t, err)
			current.latestCert = cert
		}
	}
	return current
}

type testEnv struct {
	t              *testing.T
	errgrp         *errgroup.Group
	testCtx        context.Context
	signingBackend *signing.FakeBackend
	nodes          []*testNode
	ec             *consensus.FakeEC
	net            mocknet.Mocknet
	clock          *clock.Mock
	tempDir        string // we need to ask for it before any of our cleanup hooks

	manifest manifest.Manifest
}

// waits for all nodes to reach a specific instance number.
// If the `strict` flag is enabled the check also applies to the non-running nodes
func (e *testEnv) requireInstanceEventually(instanceNumber uint64, timeout time.Duration, strict bool) {
	e.t.Helper()
	e.whileAdvancingClock(
		func() {
			require.Eventually(e.t, func() bool {
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
			}, timeout, eventualCheckInterval, "Instance %d not reached in time. Environment: %s", instanceNumber, e)
		},
	)
}

func (e *testEnv) withNodes(n int) *testEnv {
	for range n {
		e.addNode()
	}
	return e
}

func (e *testEnv) whileAdvancingClock(do func()) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.testCtx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(advanceClockEvery)
		defer ticker.Stop()
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.clock.Add(advanceClockBy)
			}
		}
	}()
	do()
}

func (e *testEnv) requireEpochFinalizedEventually(epoch int64, timeout time.Duration) {
	e.t.Helper()
	e.whileAdvancingClock(
		func() {
			require.Eventually(e.t, func() (_met bool) {
				for _, n := range e.nodes {
					if n.f3 == nil || !n.f3.IsRunning() {
						continue
					}
					cert, err := n.f3.GetLatestCert(e.testCtx)
					if cert == nil || err != nil {
						continue
					}
					if cert.ECChain.Head().Epoch >= epoch {
						return true
					}
				}
				return false
			}, timeout, eventualCheckInterval, "Epoch %d not reached in time. Environment: %s", epoch, e)
		},
	)
}

func newTestEnvironment(t *testing.T) *testEnv {
	t.Helper()

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
		env.clock.Add(500 * env.manifest.EC.Period)
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

type nodeMatcher func(*testNode) bool

var nodeMatchers = struct {
	all  nodeMatcher
	byID func(...int) nodeMatcher
}{
	all: func(*testNode) bool { return true },
	byID: func(ids ...int) nodeMatcher {
		return func(n *testNode) bool {
			for _, id := range ids {
				if n.id == id {
					return true
				}
			}
			return false
		}
	},
}

func (e *testEnv) requireF3RunningEventually(timeout time.Duration, matcher nodeMatcher) {
	e.t.Helper()
	e.whileAdvancingClock(func() {
		require.Eventually(e.t, func() bool {
			for _, n := range e.nodes {
				if !matcher(n) {
					continue
				}
				if !n.f3.IsRunning() {
					return false
				}
			}
			return true
		}, timeout, eventualCheckInterval, "F3 not running in time. Environment: %s", e)
	})
}

func (e *testEnv) requireF3NotRunningEventually(timeout time.Duration, matcher nodeMatcher) {
	e.t.Helper()
	e.whileAdvancingClock(func() {
		require.Eventually(e.t, func() bool {
			for _, n := range e.nodes {
				if !matcher(n) {
					continue
				}
				if n.f3.IsRunning() {
					return false
				}
				if n.f3.Progress().Phase != gpbft.INITIAL_PHASE {
					return false
				}
			}
			return true
		}, timeout, eventualCheckInterval, "F3 still running after timeout. Environment: %s", e)
	})
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

		e.ec = consensus.NewFakeEC(
			consensus.WithClock(e.clock),
			consensus.WithSeed(1413),
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
		require.NoError(e.t, n.f3.Start(e.testCtx))
	}

	// wait for nodes to initialize
	e.requireF3RunningEventually(eventualCheckTimeout, nodeMatchers.all)

	return e
}

func (e *testEnv) withManifest(m manifest.Manifest) *testEnv {
	e.manifest = m
	return e
}

func (e *testEnv) pauseNode(i int) {
	e.nodes[i].pause()
}

func (e *testEnv) resumeNode(i int) {
	e.nodes[i].resume()
}

func (e *testEnv) injectDatastoreFailures(i int, fn func(op string) error) {
	e.nodes[i].dsErrFunc = fn
}

func (e *testEnv) String() string {
	var out strings.Builder
	for _, n := range e.nodes {
		out.WriteString(fmt.Sprintln())
		out.WriteString(n.status().String())
	}
	return out.String()
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
