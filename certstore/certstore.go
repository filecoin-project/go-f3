package certstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/filecoin-project/go-f3/certs"
	"github.com/filecoin-project/go-f3/gpbft"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
)

var ErrCertNotFound = errors.New("certificate not found")
var ErrNotInitialized = errors.New("certstore is not initialized")

const defaultPowerTableFrequency = 60 * 24 // expected twice a day for Filecoin

var (
	certStoreLatestKey = datastore.NewKey("/latestCert")
	certStoreFirstKey  = datastore.NewKey("/firstInstance")
)

// Store is responsible for storing and relaying information about new finality certificates
type Store struct {
	mu                  sync.RWMutex
	ds                  datastore.Datastore
	firstInstance       uint64
	powerTableFrequency uint64
	subscribers         map[chan *certs.FinalityCertificate]struct{}
	latestCertificate   *certs.FinalityCertificate

	latestPowerTable gpbft.PowerEntries
}

// Internal helper function to open a certificate store that may or may not have been created.
func open(ctx context.Context, ds datastore.Datastore) (*Store, error) {
	cs := &Store{
		ds:                  namespace.Wrap(ds, datastore.NewKey("/certstore")),
		powerTableFrequency: defaultPowerTableFrequency,
		subscribers:         make(map[chan *certs.FinalityCertificate]struct{}),
	}
	err := maybeContinueDelete(ctx, ds)
	if err != nil {
		return nil, fmt.Errorf("continuing deletion: %w", err)
	}

	latestInstance, err := cs.readInstanceNumber(ctx, certStoreLatestKey)
	if errors.Is(err, datastore.ErrNotFound) {
		return cs, nil
	} else if err != nil {
		return nil, fmt.Errorf("determining latest cert: %w", err)
	}

	cs.latestCertificate, err = cs.Get(ctx, latestInstance)
	if err != nil {
		return nil, fmt.Errorf("loading latest cert: %w", err)
	}

	metrics.latestInstance.Record(ctx, int64(cs.latestCertificate.GPBFTInstance))
	metrics.latestFinalizedEpoch.Record(ctx, cs.latestCertificate.ECChain.Head().Epoch)

	return cs, nil
}

// OpenOrCreateStore opens the certificate store if it doesn't exist, or creates it. If the
// certificate store already exists but uses a different initial instance and/or power table, this
// function will return an error.
//
// The passed Datastore has to be thread safe.
func OpenOrCreateStore(ctx context.Context, ds datastore.Datastore, firstInstance uint64, initialPowerTable gpbft.PowerEntries) (*Store, error) {
	if len(initialPowerTable) == 0 {
		return nil, errors.New("cannot construct certificate store with an empty initial power table")
	}
	cs, err := open(ctx, ds)
	if err != nil {
		return nil, err
	}
	dbFirstInstance, err := cs.readInstanceNumber(ctx, certStoreFirstKey)
	if err == nil {
		if firstInstance != dbFirstInstance {
			return nil, fmt.Errorf("certificate store re-initialized with a different initial instance %d != %d", dbFirstInstance, firstInstance)
		}
		var buf bytes.Buffer
		if err := initialPowerTable.MarshalCBOR(&buf); err != nil {
			return nil, fmt.Errorf("failed to martial initial power table: %w", err)
		}
		pb, err := cs.ds.Get(ctx, cs.keyForPowerTable(firstInstance))
		if err != nil {
			return nil, fmt.Errorf("failed to load initial power table: %w", err)
		}
		if !bytes.Equal(buf.Bytes(), pb) {
			return nil, errors.New("certificate store re-initialized with the wrong power table")
		}
	} else if errors.Is(err, datastore.ErrNotFound) {
		if err := cs.putPowerTable(ctx, firstInstance, initialPowerTable); err != nil {
			return nil, fmt.Errorf("while storing the initial power table: %w", err)
		}
		if err := cs.writeInstanceNumber(ctx, certStoreFirstKey, firstInstance); err != nil {
			return nil, fmt.Errorf("while recording the first instance: %w", err)
		}
	} else {
		return nil, fmt.Errorf("failed to read initial instance number: %w", err)
	}
	cs.firstInstance = firstInstance
	if latest := cs.latestCertificate; latest != nil {
		cs.latestPowerTable, err = cs.GetPowerTable(ctx, latest.GPBFTInstance+1)
		if err != nil {
			return nil, fmt.Errorf("failed to load latest power table: %w", err)
		}
	} else {
		cs.latestPowerTable = initialPowerTable
	}

	return cs, nil
}

// CreateStore initializes a new certificate store. It will fail if the store already exists.
// The passed Datastore has to be thread safe.
func CreateStore(ctx context.Context, ds datastore.Datastore, firstInstance uint64, initialPowerTable gpbft.PowerEntries) (*Store, error) {
	if len(initialPowerTable) == 0 {
		return nil, errors.New("cannot construct certificate store with an empty initial power table")
	}
	cs, err := open(ctx, ds)
	if err != nil {
		return nil, err
	}
	if _, err := cs.readInstanceNumber(ctx, certStoreFirstKey); err == nil {
		return nil, errors.New("certificate store already initialized")
	}
	if err := cs.putPowerTable(ctx, firstInstance, initialPowerTable); err != nil {
		return nil, fmt.Errorf("while storing the initial power table: %w", err)
	}
	if err := cs.writeInstanceNumber(ctx, certStoreFirstKey, firstInstance); err != nil {
		return nil, fmt.Errorf("while recording the first instance: %w", err)
	}
	cs.firstInstance = firstInstance
	cs.latestPowerTable = initialPowerTable

	return cs, nil
}

// OpenStore opens an existing certificate store.
// The passed Datastore has to be thread safe.
// Returns ErrNotInitialized if the CertStore does not exist
func OpenStore(ctx context.Context, ds datastore.Datastore) (*Store, error) {
	cs, err := open(ctx, ds)
	if err != nil {
		return nil, err
	}
	cs.firstInstance, err = cs.readInstanceNumber(ctx, certStoreFirstKey)
	if errors.Is(err, datastore.ErrNotFound) {
		return nil, ErrNotInitialized
	}
	if err != nil {
		return nil, fmt.Errorf("getting first instance: %w", err)
	}
	latestPowerTable := cs.firstInstance
	if latest := cs.latestCertificate; latest != nil {
		latestPowerTable = latest.GPBFTInstance + 1
	}
	cs.latestPowerTable, err = cs.GetPowerTable(ctx, latestPowerTable)
	if err != nil {
		return nil, fmt.Errorf("getting latest power table: %w", err)
	}
	return cs, nil
}

// Read a big-endian unsigned integer from the specified key.
func (cs *Store) readInstanceNumber(ctx context.Context, key datastore.Key) (uint64, error) {
	b, err := cs.ds.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to read instance number at %q: %w", key, err)
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("unexpected instance number len %d != 8 at %q", len(b), key)
	}
	return binary.BigEndian.Uint64(b), nil
}

// Write a big-endian unsigned integer to the specified key.
func (cs *Store) writeInstanceNumber(ctx context.Context, key datastore.Key, value uint64) error {
	err := cs.ds.Put(ctx, key, binary.BigEndian.AppendUint64(nil, value))
	if err != nil {
		return fmt.Errorf("failed to write instance number at %q: %w", key, err)
	}
	return nil
}

// Latest returns the newest available certificate
func (cs *Store) Latest() *certs.FinalityCertificate {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.latestCertificate
}

// Get returns the FinalityCertificate at the specified instance, or an error derived from
// ErrCertNotFound.
func (cs *Store) Get(ctx context.Context, instance uint64) (*certs.FinalityCertificate, error) {
	b, err := cs.ds.Get(ctx, cs.keyForCert(instance))

	if errors.Is(err, datastore.ErrNotFound) {
		return nil, fmt.Errorf("cert at %d: %w", instance, ErrCertNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("accessing cert in datastore: %w", err)
	}

	var c certs.FinalityCertificate
	if err := c.UnmarshalCBOR(bytes.NewReader(b)); err != nil {
		return nil, fmt.Errorf("unmarshalling cert: %w", err)
	}
	return &c, err
}

// GetRange returns a range of certs from start to end inclusive by instance numbers in the
// increasing order. Only this order of traversal is supported.
//
// If it encounters missing cert, it returns a wrapped ErrCertNotFound and the available certs.
func (cs *Store) GetRange(ctx context.Context, start uint64, end uint64) ([]certs.FinalityCertificate, error) {
	if start > end {
		return nil, fmt.Errorf("start is larger than end: %d > %d", start, end)
	}
	if end-start >= math.MaxInt {
		return nil, fmt.Errorf("range %d to %d is too large", start, end)
	}

	bCerts := make([][]byte, 0, end-start+1)

	for i := start; i <= end; i++ {
		b, err := cs.ds.Get(ctx, cs.keyForCert(i))
		if errors.Is(err, datastore.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("accessing cert at %d for range request: %w", i, err)
		}

		bCerts = append(bCerts, b)
	}

	certs := make([]certs.FinalityCertificate, len(bCerts))
	for j, bCert := range bCerts {
		err := certs[j].UnmarshalCBOR(bytes.NewReader(bCert))
		if err != nil {
			return nil, fmt.Errorf("unmarshalling a cert at j=%d, instance %d: %w", j, start+uint64(j), err)
		}
	}

	if len(certs) < cap(bCerts) {
		return certs, fmt.Errorf("cert at %d: %w", start+uint64(len(bCerts)), ErrCertNotFound)
	}
	return certs, nil
}

func (cs *Store) readPowerTable(ctx context.Context, instance uint64) (gpbft.PowerEntries, error) {
	var powerTable gpbft.PowerEntries
	if b, err := cs.ds.Get(ctx, cs.keyForPowerTable(instance)); err != nil {
		return nil, fmt.Errorf("failed to load power table at instance %d: %w", instance, err)
	} else if err := powerTable.UnmarshalCBOR(bytes.NewReader(b)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal power table at instance %d: %w", instance, err)
	}
	return powerTable, nil
}

// Store the specified power table.
func (cs *Store) putPowerTable(ctx context.Context, instance uint64, powerTable gpbft.PowerEntries) error {
	var buf bytes.Buffer
	if err := powerTable.MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("marshalling power table instance %d: %w", instance, err)
	}
	if err := cs.ds.Put(ctx, cs.keyForPowerTable(instance), buf.Bytes()); err != nil {
		return fmt.Errorf("putting power table instance %d: %w", instance, err)
	}
	return nil
}

// GetPowerTable returns the power table (committee) used to validate the specified instance.
func (cs *Store) GetPowerTable(ctx context.Context, instance uint64) (gpbft.PowerEntries, error) {
	if instance < cs.firstInstance {
		return nil, fmt.Errorf("cannot return a power table before the first instance: %d", cs.firstInstance)
	}

	// Copy a reference to both the latest cert and latest power table while holding
	// the lock to guarantee order in case the latest changes while the requested
	// power table is being retrieved.
	cs.mu.RLock()
	latestCert := cs.latestCertificate
	latestPowerTable := cs.latestPowerTable
	cs.mu.RUnlock()

	nextCertInstance := cs.firstInstance
	if latestCert != nil {
		nextCertInstance = latestCert.GPBFTInstance + 1
	}
	if instance > nextCertInstance {
		return nil, fmt.Errorf("cannot return future power table for instance %d > %d", instance, nextCertInstance)
	}
	if instance == nextCertInstance && len(latestPowerTable) != 0 {
		// Note that the latestPowerTable may be nil/empty, which indicates the certstore
		// is being opened. Hence, the strict check for non-zero length.
		return latestPowerTable, nil
	}
	// Technically, it's possible to also optimize for the case where the instance is
	// equal to the latest certificate instance. This is by "subtracting" the latest
	// cert power table delta from the latest power table. But it's unclear if that
	// optimization is necessary. Observing the runtime metrics should make it clear
	// if it is. For now, YAGNI.

	// We store every `powerTableFrequency` power tables. Find the nearest multiple smaller than
	// the requested instance.
	startInstance := max(instance-instance%cs.powerTableFrequency, cs.firstInstance)

	powerTable, err := cs.readPowerTable(ctx, startInstance)
	if err != nil {
		return nil, fmt.Errorf("failed to find expected power table for instance %d: %w", startInstance, err)
	}
	if startInstance == instance {
		return powerTable, nil
	}
	// Load the power table diffs up till (but not including) the target instance.
	certificates, err := cs.GetRange(ctx, startInstance, instance-1)
	if err != nil {
		return nil, err
	}

	// Apply the diffs and return the result.
	deltas := make([]certs.PowerTableDiff, len(certificates))
	for i := range certificates {
		deltas[i] = certificates[i].PowerTableDelta
	}
	powerTable, err = certs.ApplyPowerTableDiffs(powerTable, deltas...)
	if err != nil {
		return nil, fmt.Errorf("applying power deltas: %w", err)
	}
	return powerTable, err
}

func (*Store) keyForCert(i uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("/certs/%016X", i))
}

func (*Store) keyForPowerTable(i uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("/power/%016X", i))
}

// Put saves a certificate in a store and notifies listeners.
// It returns an error if the certificate is either:
//
// 1. Before the initial instance that the certificate store was initialized with.
// 2. More than one instance after the last certificate stored.
func (cs *Store) Put(ctx context.Context, cert *certs.FinalityCertificate) error {
	if cert.GPBFTInstance < cs.firstInstance {
		return fmt.Errorf("certificate store only stores certificates on or after instance %d", cs.firstInstance)
	}

	// Basic validation just to make sure the certificate is sane. We don't do a full validation
	// because that should already have been done by the caller.
	if cert.ECChain.IsZero() {
		return fmt.Errorf("finality certificate for instance %d is for bottom", cert.GPBFTInstance)
	} else if err := cert.ECChain.Validate(); err != nil {
		return fmt.Errorf("invalid chain in finality certificate: %w", err)
	}

	// Take a lock to ensure ordering.
	cs.mu.Lock()
	defer cs.mu.Unlock()

	nextCert := cs.firstInstance
	if latestCert := cs.latestCertificate; latestCert != nil {
		nextCert = latestCert.GPBFTInstance + 1
	}
	if cert.GPBFTInstance > nextCert {
		return fmt.Errorf("attempted to add cert at %d, expected %d", cert.GPBFTInstance, nextCert)
	}
	if cert.GPBFTInstance < nextCert {
		return nil
	}

	// The instance is exactly latest + 1

	// Compute the next power table (if it has changed).
	newPowerTable := cs.latestPowerTable
	if len(cert.PowerTableDelta) > 0 {
		var err error
		newPowerTable, err = certs.ApplyPowerTableDiffs(cs.latestPowerTable, cert.PowerTableDelta)
		if err != nil {
			return fmt.Errorf("failed to apply power table delta for instance %d: %w", cert.GPBFTInstance, err)
		}
	}

	// Check the power table CID. This _should_ already have been checked (we're not validating
	// the entire finality certificate, but errors here will compound and be difficult to fix
	// later.
	if ptCid, err := certs.MakePowerTableCID(newPowerTable); err != nil {
		return err
	} else if ptCid != cert.SupplementalData.PowerTable {
		return fmt.Errorf("new power table differs from expected power table: %s != %s", ptCid, cert.SupplementalData.PowerTable)
	}

	// Double check that we're not killing the network.
	if len(newPowerTable) == 0 {
		return fmt.Errorf("finality certificate for instance %d would empty the power table", cert.GPBFTInstance)
	}

	// Write the cert/power table.
	var buf bytes.Buffer
	if err := cert.MarshalCBOR(&buf); err != nil {
		return fmt.Errorf("marshalling cert instance %d: %w", cert.GPBFTInstance, err)
	}

	if err := cs.ds.Put(ctx, cs.keyForCert(cert.GPBFTInstance), buf.Bytes()); err != nil {
		return fmt.Errorf("putting the cert: %w", err)
	}

	// The new power table is the power table to validate the _next_ instance.
	if (cert.GPBFTInstance+1)%cs.powerTableFrequency == 0 {
		if err := cs.putPowerTable(ctx, cert.GPBFTInstance+1, newPowerTable); err != nil {
			return err
		}
	}

	// Finally, advance the latest instance pointer (always do this last) and publish.
	if err := cs.writeInstanceNumber(ctx, certStoreLatestKey, cert.GPBFTInstance); err != nil {
		return fmt.Errorf("putting recording the latest GPBFT instance: %w", err)
	}

	cs.latestPowerTable = newPowerTable
	cs.latestCertificate = cert
	for ch := range cs.subscribers {
		// Always drain first.
		select {
		case <-ch:
		default:
		}
		// Then write the latest certificate.
		ch <- cs.latestCertificate
	}

	metrics.latestInstance.Record(ctx, int64(cert.GPBFTInstance))
	metrics.tipsetsPerInstance.Record(ctx, int64(len(cert.ECChain.Suffix())))
	metrics.latestFinalizedEpoch.Record(ctx, cert.ECChain.Head().Epoch)

	return nil
}

// Subscribe subscribes to new certificate notifications. When read, it will always return the
// latest not-yet-seen certificate (including the latest certificate when Subscribe is first
// called, if we have any) but it will drop intermediate certificates. If you need all the
// certificates, you should keep track of the last certificate you received and call GetRange to get
// the ones between.
//
// The caller must call the closer to unsubscribe and release resources.
func (cs *Store) Subscribe() (out <-chan *certs.FinalityCertificate, closer func()) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	ch := make(chan *certs.FinalityCertificate, 1)
	if cs.latestCertificate != nil {
		ch <- cs.latestCertificate
	}
	cs.subscribers[ch] = struct{}{}
	return ch, func() {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		if _, ok := cs.subscribers[ch]; ok {
			delete(cs.subscribers, ch)
			close(ch)
		}
	}
}

var tombstoneKey = datastore.NewKey("/tombstone")

func maybeContinueDelete(ctx context.Context, ds datastore.Datastore) error {
	ok, err := ds.Has(ctx, tombstoneKey)
	if err != nil {
		return fmt.Errorf("checking tombstoneKey: %w", err)
	}
	if !ok {
		// no tombstone, exit
		return nil
	}

	qr, err := ds.Query(ctx, query.Query{KeysOnly: true})
	if err != nil {
		return fmt.Errorf("starting a query for certs: %w", err)
	}
	for r := range qr.Next() {
		key := datastore.NewKey(r.Key)
		if key == tombstoneKey {
			continue
		}
		err := ds.Delete(ctx, key)
		if err != nil {
			return fmt.Errorf("error while deleting: %w", err)
		}
	}
	err = ds.Delete(ctx, tombstoneKey)
	if err != nil {
		return fmt.Errorf("error while deleting tombstone: %w", err)
	}
	return nil
}

// DeleteAll is used to remove all certificates from the store and clean it for a new instance
// to be able to use it from scratch.
func (cs *Store) DeleteAll(ctx context.Context) error {
	err := cs.ds.Put(ctx, tombstoneKey, []byte("tombstone"))
	if err != nil {
		return fmt.Errorf("creating a tombstone: %w", err)
	}

	return maybeContinueDelete(ctx, cs.ds)
}

// Delete removes all asset belonging to an instance.
func (cs *Store) Delete(ctx context.Context, instance uint64) error {
	if err := cs.ds.Delete(ctx, cs.keyForCert(instance)); err != nil {
		return err
	}
	return cs.ds.Delete(ctx, cs.keyForPowerTable(instance))
}
