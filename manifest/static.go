package manifest

import (
	"context"
)

var _ ManifestProvider = (*StaticManifestProvider)(nil)

// Static manifest provider that doesn't allow any changes
// in runtime to the initial manifest set in the provider
type StaticManifestProvider struct {
	ch <-chan *Manifest
}

func NewStaticManifestProvider(m *Manifest) ManifestProvider {
	ch := make(chan *Manifest, 1)
	ch <- m
	return &StaticManifestProvider{ch: ch}
}

func (m *StaticManifestProvider) Start(context.Context) error       { return nil }
func (m *StaticManifestProvider) Stop(context.Context) error        { return nil }
func (m *StaticManifestProvider) ManifestUpdates() <-chan *Manifest { return m.ch }
