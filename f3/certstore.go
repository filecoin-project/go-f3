package f3

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"golang.org/x/xerrors"
)

// TODO: setup cbor-gen
type Cert struct {
	Instance uint64
}

func (c *Cert) UnmarshalBinary(b []byte) error {
	// temp until cbor-gen
	if len(b) != 8 {
		return xerrors.Errorf("expected length 8 got %d", len(b))
	}
	c.Instance = binary.LittleEndian.Uint64(b)
	return nil
}
func (c Cert) MarshalBinary() ([]byte, error) {
	return binary.LittleEndian.AppendUint64([]byte{}, c.Instance), nil
}

var ErrCertAlreadyExists = errors.New("certificate already exists in the CertStore")

// CertStore is responsible for storying and relaying information about new finality certificates
type CertStore struct {
	lk sync.Mutex
	ds datastore.Datastore

	// TODO: add libp2p/go-eventbus
}

func NewCertStore(ds datastore.Datastore) (*CertStore, error) {
	cs := &CertStore{
		ds: namespace.Wrap(ds, datastore.NewKey("/certstore")),
	}
	return cs, nil
}

func (cs *CertStore) Get(ctx context.Context, instance uint64) (*Cert, error) {
	cs.lk.Lock()
	b, err := cs.ds.Get(ctx, cs.keyForInstance(instance))
	cs.lk.Unlock()

	if err != nil {
		return nil, xerrors.Errorf("accessing cert in datastore: %w", err)
	}

	var c Cert
	if err := c.UnmarshalBinary(b); err != nil {
		return nil, xerrors.Errorf("unmarshalling cert: %w", err)
	}
	return &c, err
}

func (_ *CertStore) keyForInstance(i uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("%X", i))
}

// Put saves a certificate in a store and notifies listeners.
// It errors if the certificate is already in the store
func (cs *CertStore) Put(ctx context.Context, c *Cert) error {
	certBytes, err := c.MarshalBinary()
	if err != nil {
		return xerrors.Errorf("marshalling cert instance %d: %w", c.Instance, err)
	}
	if err := cs.putInner(ctx, c.Instance, certBytes); err != nil {
		return xerrors.Errorf("saving cert at %d: %w", c.Instance, err)
	}

	// TODO notify listeners

	return nil
}
func (cs *CertStore) putInner(ctx context.Context, instance uint64, certBytes []byte) error {
	key := cs.keyForInstance(instance)

	cs.lk.Lock()
	defer cs.lk.Unlock()

	exists, err := cs.ds.Has(ctx, key)
	if err != nil {
		return xerrors.Errorf("checking existence of cert: %w", err)
	}
	if exists {
		return ErrCertAlreadyExists
	}

	if err := cs.ds.Put(ctx, key, certBytes); err != nil {
		return xerrors.Errorf("putting the cert: %w", err)
	}
	return nil
}

// TODO listen for new finality certs
