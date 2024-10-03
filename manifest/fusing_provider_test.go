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

// Static manifest provider that doesn't allow any changes
// in runtime to the initial manifest set in the provider
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
	prov, err := manifest.NewFusingManifestProvider(ctx,
		fakeEc, (testManifestProvider)(manifestCh), initialManifest)
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

	// Now update the dynamic manifest. We shouldn't receive it.
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
