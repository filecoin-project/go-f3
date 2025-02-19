package manifest_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-f3/internal/clock"
	"github.com/filecoin-project/go-f3/internal/consensus"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/stretchr/testify/require"
)

type testManifestProvider chan *manifest.Manifest

func (testManifestProvider) Start(context.Context) error { return nil }
func (testManifestProvider) Stop(context.Context) error  { return nil }
func (m testManifestProvider) ManifestUpdates() <-chan *manifest.Manifest {
	return (<-chan *manifest.Manifest)(m)
}

func TestFusingManifestProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, clk := clock.WithMockClock(ctx)
	t.Cleanup(cancel)

	initialManifest := manifest.LocalDevnetManifest()
	initialManifest.BootstrapEpoch = 1000
	initialManifest.EC.Finality = 900
	otherManifest := *initialManifest
	otherManifest.BootstrapEpoch -= 10

	fakeEc := consensus.NewFakeEC(ctx)
	manifestCh := make(chan *manifest.Manifest, 10)
	priorityManifestCh := make(chan *manifest.Manifest, 1)
	priorityManifestProvider := testManifestProvider(priorityManifestCh)
	priorityManifestCh <- initialManifest

	prov, err := manifest.NewFusingManifestProvider(ctx,
		fakeEc, (testManifestProvider)(manifestCh), priorityManifestProvider)
	require.NoError(t, err)

	require.NoError(t, prov.Start(ctx))

	// No updates queued initially.
	require.Empty(t, prov.ManifestUpdates())

	// We should get the manifest update.
	select {
	case manifestCh <- &otherManifest:
	default:
		t.Fatal("failed to enqueue manifest update")
	}
	require.True(t, otherManifest.Equal(<-prov.ManifestUpdates()))

	// Add some time but not enough to switch manifests. We shouldn't get the update.
	clk.Add(initialManifest.EC.Period * 90)
	select {
	case <-prov.ManifestUpdates():
		t.Fatal("did not expect a manifest update")
	case <-time.After(time.Second):
	}

	// Add enough time to switch to the static manifest (within one finality of the bootstrap
	// epoch).
	clk.Add(initialManifest.EC.Period * 10)
	select {
	case m := <-prov.ManifestUpdates():
		require.True(t, m.Equal(initialManifest), "expected to receive the static manifest")
	case <-time.After(time.Second):
		t.Fatal("expected a manifest update")
	}

	// Now update the secondary manifest. We shouldn't receive it.
	select {
	case manifestCh <- &otherManifest:
	default:
		t.Fatal("failed to enqueue manifest update")
	}

	select {
	case <-prov.ManifestUpdates():
		t.Fatal("did not expect a manifest update")
	case <-time.After(time.Second):
	}
}

// Test that starting and stopping the fusing manifest provider works correctly, even if we never
// "fuse".
func TestFusingManifestProviderStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, _ = clock.WithMockClock(ctx)
	t.Cleanup(cancel)

	initialManifest := manifest.LocalDevnetManifest()
	initialManifest.BootstrapEpoch = 1000

	fakeEc := consensus.NewFakeEC(ctx)
	manifestCh := make(chan *manifest.Manifest, 1)
	priorityManifestCh := make(chan *manifest.Manifest, 1)
	priorityManifestProvider := testManifestProvider(priorityManifestCh)
	priorityManifestCh <- initialManifest

	prov, err := manifest.NewFusingManifestProvider(ctx,
		fakeEc, (testManifestProvider)(manifestCh), priorityManifestProvider)
	require.NoError(t, err)

	require.NoError(t, prov.Start(ctx))
	require.NoError(t, prov.Stop(ctx))
}

func TestFusingManifestProviderSwitchToPriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, clk := clock.WithMockClock(ctx)
	t.Cleanup(cancel)

	initialManifest := manifest.LocalDevnetManifest()
	initialManifest.BootstrapEpoch = 2000
	initialManifest.EC.Finality = 900

	fakeEc := consensus.NewFakeEC(ctx)
	manifestCh := make(chan *manifest.Manifest, 10)

	priorityManifestCh := make(chan *manifest.Manifest, 1)
	priorityManifestProvider := testManifestProvider(priorityManifestCh)
	priorityManifestCh <- nil

	prov, err := manifest.NewFusingManifestProvider(ctx,
		fakeEc, (testManifestProvider)(manifestCh), priorityManifestProvider)
	require.NoError(t, err)

	require.NoError(t, prov.Start(ctx))
	priorityManifestCh <- initialManifest

	// Create and push a secondary manifest with bootstrap epoch < 1100
	secondaryManifest := *initialManifest
	secondaryManifest.BootstrapEpoch = 1000
	select {
	case manifestCh <- &secondaryManifest:
	default:
		t.Fatal("failed to enqueue secondary manifest")
	}

	select {
	case m := <-prov.ManifestUpdates():
		require.True(t, m.Equal(&secondaryManifest), "expected secondary manifest")
	case <-time.After(time.Second):
		t.Fatal("expected a manifest update")
	}

	// Add time to reach the priority manifest switch epoch
	clk.Add(initialManifest.EC.Period * 1200)
	for i := 0; i < 10; i++ {
		// fixes weird quirk with fake time
		// where the initial manifest doesn't get processed before the initial clk.Add
		// and the timer doesn't fire until another clk.Add
		clk.Add(1)
	}
	t.Logf("clck now: %s", clk.Now())
	select {
	case m := <-prov.ManifestUpdates():
		require.True(t, m.Equal(initialManifest), "expected to receive the priority manifest")
	case <-time.After(time.Second):
		t.Fatal("expected a manifest update")
	}
}
