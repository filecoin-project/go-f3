package manifest

import (
	"context"

	"github.com/filecoin-project/go-f3/gpbft"
)

// Static manifest provider that doesn't allow any changes
// in runtime to the initial manifest set in the provider
type StaticManifestProvider struct {
	manifest Manifest
}

var _ ManifestProvider = (*StaticManifestProvider)(nil)

func NewStaticManifestProvider(m Manifest) ManifestProvider {
	return &StaticManifestProvider{manifest: m}
}

func (m *StaticManifestProvider) GpbftOptions() []gpbft.Option {
	return m.manifest.GpbftOptions()
}

func (m *StaticManifestProvider) Manifest() Manifest {
	return m.manifest
}

func (m *StaticManifestProvider) Run(ctx context.Context, errCh chan error) {}

func (m *StaticManifestProvider) SetManifestChangeCallback(mc OnManifestChange) {}
