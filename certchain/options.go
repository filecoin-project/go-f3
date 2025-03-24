package certchain

import (
	"errors"

	"github.com/filecoin-project/go-f3/ec"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/manifest"
)

type Option func(*options) error

type SignVerifier interface {
	gpbft.Signer
	gpbft.Verifier
}

type options struct {
	ec   ec.Backend
	m    *manifest.Manifest
	sv   SignVerifier
	seed int64
}

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		m: manifest.LocalDevnetManifest(),
	}
	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	if opts.ec == nil {
		return nil, errors.New("ec backend must be specified")
	}
	if opts.sv == nil {
		return nil, errors.New("sign verifier must be specified")
	}
	return opts, nil
}

func WithEC(ec ec.Backend) Option {
	return func(o *options) error {
		o.ec = ec
		return nil
	}
}

func WithManifest(m *manifest.Manifest) Option {
	return func(o *options) error {
		o.m = m
		return nil
	}
}

func WithSignVerifier(sv SignVerifier) Option {
	return func(o *options) error {
		o.sv = sv
		return nil
	}
}

func WithSeed(seed int64) Option {
	return func(o *options) error {
		o.seed = seed
		return nil
	}
}
