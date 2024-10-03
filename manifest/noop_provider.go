package manifest

import (
	"context"
)

var _ ManifestProvider = (*NoopManifestProvider)(nil)

type NoopManifestProvider struct{}

func (NoopManifestProvider) Start(context.Context) error       { return nil }
func (NoopManifestProvider) Stop(context.Context) error        { return nil }
func (NoopManifestProvider) ManifestUpdates() <-chan *Manifest { return nil }
