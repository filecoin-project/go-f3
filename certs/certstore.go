package certs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"

	"github.com/Kubuxu/go-broadcast"
	"golang.org/x/xerrors"
)

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
	return binary.LittleEndian.AppendUint64(nil, c.Instance), nil
}

var ErrCertNotFound = errors.New("certificate not found")

// Store is responsible for storing and relaying information about new finality certificates
type Store struct {
	writeLk  sync.Mutex
	ds       datastore.Datastore
	busCerts broadcast.Channel[*Cert]
}

// NewStore creates a certstore
// The passed Datastore has to be thread safe.
func NewStore(ctx context.Context, ds datastore.Datastore) (*Store, error) {
	cs := &Store{
		ds: namespace.Wrap(ds, datastore.NewKey("/certstore")),
	}
	latestCert, err := cs.loadLatest(ctx)
	if err != nil {
		return nil, xerrors.Errorf("loading latest cert: %w", err)
	}
	if latestCert != nil {
		cs.busCerts.Publish(latestCert)
	}

	return cs, nil
}

var certStoreLatestKey = datastore.NewKey("/latestCert")

func (cs *Store) loadLatest(ctx context.Context) (*Cert, error) {
	cb, err := cs.ds.Get(ctx, certStoreLatestKey)
	if errors.Is(err, datastore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, xerrors.Errorf("getting latest cert: %w", err)
	}
	var c Cert
	err = c.UnmarshalBinary(cb)
	if err != nil {
		return nil, xerrors.Errorf("unmarshalling latest cert: %w", err)
	}
	return &c, err
}

// Latest returns the newest available certificate
func (cs *Store) Latest() *Cert {
	return cs.busCerts.Last()
}

func (cs *Store) Get(ctx context.Context, instance uint64) (*Cert, error) {
	b, err := cs.ds.Get(ctx, cs.keyForInstance(instance))

	if errors.Is(err, datastore.ErrNotFound) {
		return nil, xerrors.Errorf("cert at %d: %w", instance, ErrCertNotFound)
	}
	if err != nil {
		return nil, xerrors.Errorf("accessing cert in datastore: %w", err)
	}

	var c Cert
	if err := c.UnmarshalBinary(b); err != nil {
		return nil, xerrors.Errorf("unmarshalling cert: %w", err)
	}
	return &c, err
}

// GetRange returns a range of certs from start to end inclusive by instance numbers in the
// increasing order. Only this order of traversal is supported.
// If it encounters missing cert, it returns a wrapped ErrCertNotFound and the available certs
func (cs *Store) GetRange(ctx context.Context, start uint64, end uint64) ([]Cert, error) {
	if start > end {
		return nil, xerrors.Errorf("start is larger then end: %d > %d", start, end)
	}
	if end-start > uint64(math.MaxInt)-1 {
		return nil, xerrors.Errorf("range %d to %d is too large", start, end)
	}

	bCerts := make([][]byte, 0, end-start+1)

	for i := start; i <= end; i++ {
		b, err := cs.ds.Get(ctx, cs.keyForInstance(i))
		if errors.Is(err, datastore.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf("accessing cert at %d for range request: %w", i, err)
		}

		bCerts = append(bCerts, b)
	}

	certs := make([]Cert, len(bCerts))
	for j, bCert := range bCerts {
		err := certs[j].UnmarshalBinary(bCert)
		if err != nil {
			return nil, xerrors.Errorf("unmarshalling a cert at j=%d, instance %d: %w", j, start+uint64(j), err)
		}
	}

	if len(certs) < cap(bCerts) {
		return certs, xerrors.Errorf("cert at %d: %w", start+uint64(len(bCerts)), ErrCertNotFound)
	}
	return certs, nil
}

func (_ *Store) keyForInstance(i uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("/certs/%016X", i))
}

// Put saves a certificate in a store and notifies listeners.
// It errors if adding a cert would create a gap
func (cs *Store) Put(ctx context.Context, cert *Cert) error {
	key := cs.keyForInstance(cert.Instance)

	exists, err := cs.ds.Has(ctx, key)
	if err != nil {
		return xerrors.Errorf("checking existence of cert: %w", err)
	}
	if exists {
		return nil
	}

	certBytes, err := cert.MarshalBinary()
	if err != nil {
		return xerrors.Errorf("marshalling cert instance %d: %w", cert.Instance, err)
	}

	cs.writeLk.Lock()
	defer cs.writeLk.Unlock()

	if cs.Latest() != nil && cert.Instance > cs.Latest().Instance &&
		cert.Instance != cs.Latest().Instance+1 {
		return xerrors.Errorf("attempted to add cert at %d but the previous one is %d",
			cert.Instance, cs.Latest().Instance)
	}

	if err := cs.ds.Put(ctx, key, certBytes); err != nil {
		return xerrors.Errorf("putting the cert: %w", err)
	}
	if err := cs.ds.Put(ctx, certStoreLatestKey, certBytes); err != nil {
		return xerrors.Errorf("putting the cert: %w", err)
	}
	cs.busCerts.Publish(cert) // Publish within the lock to ensure ordering

	return nil
}

// SubscribeForNewCerts is used to subscribe to the broadcast channel.
// If the passed channel is full at any point, it will be dropped from subscription and closed.
// To stop subscribing, either the closer function can be used or the channel can be abandoned.
// Passing a channel multiple times to the Subscribe function will result in a panic.
// The channel will receive new certificates sequentially.
func (cs *Store) SubscribeForNewCerts(ch chan<- *Cert) (last *Cert, closer func()) {
	return cs.busCerts.Subscribe(ch)
}
