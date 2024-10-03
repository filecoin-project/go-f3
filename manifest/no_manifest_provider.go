package manifest

import (
	"context"
)

var _ ManifestProvider = (*NoManifestProvider)(nil)

type NoManifestProvider struct{}

func (NoManifestProvider) Start(context.Context) error       { return nil }
func (NoManifestProvider) Stop(context.Context) error        { return nil }
func (NoManifestProvider) ManifestUpdates() <-chan *Manifest { return nil }
