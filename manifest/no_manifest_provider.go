package manifest

import (
	"context"
)

var _ ManifestProvider = (*NoManifestProvider)(nil)

type NoManifestProvider struct{}

func (m NoManifestProvider) Start(context.Context) error       { return nil }
func (m NoManifestProvider) Stop(context.Context) error        { return nil }
func (m NoManifestProvider) ManifestUpdates() <-chan *Manifest { return nil }
