package observer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/certexchange"
	"github.com/filecoin-project/go-f3/certexchange/polling"
	"github.com/filecoin-project/go-f3/certstore"
	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/filecoin-project/go-f3/internal/lotus"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	sync2 "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/marcboeker/go-duckdb"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

var (
	logger = log.Logger("f3/observer")

	//go:embed schema.sql
	schema string
)

type Observer struct {
	*options
	stop func() error
	db   *sql.DB
	qs   http.Server
	dht  *dht.IpfsDHT

	messageObserved chan *Message
	gMsgEncoding    *encoding.ZSTD[*gpbft.PartialGMessage]

	dbConnector           *duckdb.Connector
	dbLatestMessages      *duckdb.Appender
	dbConnection          driver.Conn
	unflushedMessageCount int
	lastFlushedMessagesAt time.Time

	dbFinalityCertificates    *duckdb.Appender
	unflushedCertificateCount int
	lastFlushedCertificatesAt time.Time

	chainExchangeObserved chan *chainexchange.Message
	ceMsgEncoding         *encoding.ZSTD[*chainexchange.Message]
}

func New(o ...Option) (*Observer, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	gMsgEncoding, err := encoding.NewZSTD[*gpbft.PartialGMessage]()
	if err != nil {
		return nil, err
	}

	ceMsgEncoding, err := encoding.NewZSTD[*chainexchange.Message]()
	if err != nil {
		return nil, err
	}

	return &Observer{
		options:               opts,
		messageObserved:       make(chan *Message, opts.messageBufferSize),
		gMsgEncoding:          gMsgEncoding,
		chainExchangeObserved: make(chan *chainexchange.Message, opts.chainExchangeBufferSize),
		ceMsgEncoding:         ceMsgEncoding,
	}, nil
}

func (o *Observer) Start(ctx context.Context) error {
	if err := o.initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialise observer: %w", err)
	}

	ctx, stop := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return o.observeMessages(ctx) })
	eg.Go(func() error { return o.observeChainExchanges(ctx) })
	eg.Go(func() error { return o.observeFinalityCertificates(ctx) })
	eg.Go(func() error { return o.stayConnected(ctx) })
	eg.Go(o.listenAndServeQueries)
	o.stop = func() error {
		stop()
		return multierr.Combine(
			o.dbLatestMessages.Close(),
			o.dbFinalityCertificates.Close(),
			o.dbConnection.Close(),
			o.dbConnector.Close(),
			o.db.Close(),
			eg.Wait())
	}
	return nil
}

func (o *Observer) initialize(ctx context.Context) error {
	var err error
	if o.pubSub == nil {
		// Set up pubsub to listen for GPBFT messages.
		if o.pubSub, err = pubsub.NewGossipSub(ctx, o.host,
			pubsub.WithPeerExchange(true),
			pubsub.WithFloodPublish(true),
			pubsub.WithMessageIdFn(psutil.GPBFTMessageIdFn),
			pubsub.WithPeerScore(psutil.PubsubPeerScoreParams, psutil.PubsubPeerScoreThresholds),
		); err != nil {
			return fmt.Errorf("failed to initialise pubsub: %w", err)
		}
	}
	if o.connectivityDHTThreshold > 0 {
		opts := []dht.Option{dht.Mode(dht.ModeAuto),
			dht.Validator(
				record.NamespacedValidator{
					"pk": record.PublicKeyValidator{},
				}),
			dht.ProtocolPrefix("/fil/kad/" + "testnetnet"), // should be base network name but we don't have manifest
			dht.QueryFilter(dht.PublicQueryFilter),
			dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
			dht.DisableProviders(),
			dht.DisableValues()}
		d, err := dht.New(
			ctx, o.host, opts...,
		)
		if err != nil {
			return fmt.Errorf("failed to initialise DHT: %w", err)
		}
		o.dht = d
	}

	// Set up database connection.
	o.dbConnector, err = duckdb.NewConnector(o.dataSourceName, nil)
	if err != nil {
		return fmt.Errorf("failed to create duckdb connector: %w", err)
	}
	o.db = sql.OpenDB(o.dbConnector)

	// Create database schema.
	if _, err := o.db.ExecContext(ctx, schema); err != nil {
		logger.Errorw("Failed to create schema", "err", err)
		return err
	}

	// Create `messages` view, a union of latest messages and the ones rotated into
	// parquet.
	includeParquetFiles, err := o.anyParquetFilesPresent()
	if err != nil {
		// OK to ignore this error since at every rotate view is recreated.
		logger.Errorw("Failed to check if any parquet files exist", "err", err)
		includeParquetFiles = false
	}
	if err := o.createOrReplaceMessagesView(ctx, includeParquetFiles); err != nil {
		logger.Errorw("failed to create or replace messages view", "err", err)
		return err
	}

	// Set up appender used for batch insertion of messages observed.
	o.dbConnection, err = o.dbConnector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to duckdb: %w", err)
	}
	o.dbLatestMessages, err = duckdb.NewAppenderFromConn(o.dbConnection, "", "latest_messages")
	if err != nil {
		return fmt.Errorf("failed to create duckdb appender for latest_messages: %w", err)
	}

	o.dbFinalityCertificates, err = duckdb.NewAppenderFromConn(o.dbConnection, "", "finality_certificates")
	if err != nil {
		return fmt.Errorf("failed to create duckdb appender for finality_certificates: %w", err)
	}

	// If connectivity check interval is enabled, repair connections once to avoid
	// waiting for the ticker.
	if o.connectivityCheckInterval > 0 {
		o.repairConnectivity(ctx)
	}

	// Set up query server.
	o.qs.Addr = o.queryServerListenAddress
	o.qs.ReadTimeout = o.queryServerReadTimeout
	o.qs.Handler = o.serveMux()

	return nil
}

func (o *Observer) createOrReplaceMessagesView(ctx context.Context, includeParquet bool) error {
	// Creating a view with union of golb *.parquet selection will fail if there are
	// no parquet files exist. Therefore, check if there is any parquet file and
	// adjust the creation query accordingly.
	includeParquetFiles := ""
	if includeParquet {
		includeParquetFiles = fmt.Sprintf("UNION ALL SELECT * FROM '%s'", filepath.Join(o.rotatePath, "*.parquet"))
	}
	createView := fmt.Sprintf(`CREATE VIEW IF NOT EXISTS messages AS SELECT * FROM latest_messages %s`, includeParquetFiles)
	_, err := o.db.ExecContext(ctx, createView)
	return err

	// TODO: maybe add a selection window to limit the view to messages from the last
	//       week or something along those lines.
}

func (o *Observer) observeMessages(ctx context.Context) error {
	rotation := time.NewTicker(o.rotateInterval)
	flush := time.NewTicker(o.maxBatchDelay)
	stopObserverForNetwork, err := o.startMessageObserverFor(ctx, o.networkName)
	if err != nil {
		return fmt.Errorf("failed to start observer for network %s: %w", o.networkName, err)
	}

	defer func() {
		rotation.Stop()
		flush.Stop()
		stopObserverForNetwork()
	}()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return nil
		case om := <-o.messageObserved:
			if err := o.storeMessage(ctx, om); err != nil {
				logger.Errorw("Failed to store message", "message", om, "err", err)
				continue
			}
			logger.Debugw("Observed message", "message", om)
		case <-rotation.C:
			if err := o.rotateMessages(ctx); err != nil {
				logger.Errorw("Failed to rotate latest messages", "err", err)
			}
		case <-flush.C:
			if err := o.tryFlushMessages(); err != nil {
				logger.Errorw("Failed to flush observed messages", "err", err)
			}
		}
	}
	return nil
}

func (o *Observer) observeChainExchanges(ctx context.Context) error {
	flush := time.NewTicker(o.maxBatchDelay)
	stopObserverForNetwork, err := o.startChainExchangeObserverFor(ctx, o.networkName)
	if err != nil {
		return fmt.Errorf("failed to start observer for network %s: %w", o.networkName, err)
	}

	defer stopObserverForNetwork()

	seenKeys := make(map[string]struct{})
	bufferedChains := make([]*ChainExchange, 0, o.maxBatchSize)

	tryStoreAllChainExchanges := func() {
		start := time.Now()
		if err := o.storeAllChainExchanges(ctx, bufferedChains); err != nil {
			logger.Errorw("Failed to store chain exchanges", "count", len(bufferedChains), "err", err)
			// Don't clear; let it retry upon the next message.
		} else {
			logger.Infow("Stored batch of chain exchanges", "count", len(bufferedChains), "took", time.Since(start))
			bufferedChains = bufferedChains[:0]
			maps.Clear(seenKeys)
		}
	}

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return nil
		case oc := <-o.chainExchangeObserved:
			timestamp := time.UnixMilli(oc.Timestamp)
			// Only key on vote value key instead of vote value key plus instance. Because,
			// the purpose of observing the chain exchanges is to be able to infer what a
			// vote value key corresponds to. We can use the flow of messages captured to
			// infer if an instance is re-using a key from previous instances.
			allPrefixes := oc.Chain.AllPrefixes()
			for i := len(allPrefixes) - 1; i >= 0 && ctx.Err() == nil; i-- {
				prefix := allPrefixes[i]
				key := prefix.Key()
				bufferKey := string(key[:])
				if _, exists := seenKeys[bufferKey]; exists {
					break
				}
				seenKeys[bufferKey] = struct{}{}
				exchange := newChainExchange(timestamp, o.networkName, oc.Instance, prefix)
				bufferedChains = append(bufferedChains, exchange)
				logger.Debugw("Observed chain exchange message", "message", exchange)
				if len(bufferedChains) >= o.maxBatchSize {
					tryStoreAllChainExchanges()
				}
			}
		case <-flush.C:
			tryStoreAllChainExchanges()
		}
	}
	return nil
}

func (o *Observer) storeAllChainExchanges(ctx context.Context, exchanges []*ChainExchange) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
        INSERT OR IGNORE INTO chain_exchanges (Timestamp, NetworkName, Instance, VoteValueKey, VoteValue)
        VALUES (?, ?, ?, ?, ?::json)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to prepare statement while storing chain exchanges: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			logger.Errorw("Failed to close prepared statement while storing chain exchanges", "err", err)
		}
	}()

	for row, cx := range exchanges {
		voteAsJson, err := json.Marshal(cx.VoteValue)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to marshal chain exchange vote value at row %d: %w", row, err)
		}
		_, err = stmt.Exec(
			cx.Timestamp,
			cx.NetworkName,
			cx.Instance,
			cx.VoteValueKey,
			string(voteAsJson),
		)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to insert chain exchange at row %d: %w", row, err)
		}
	}
	return tx.Commit()
}

func (o *Observer) observeFinalityCertificates(ctx context.Context) error {
	if o.initialPowerTableCID == cid.Undef {
		logger.Warn("Initial power table CID is not set. Finality certificates will not be collected.")
		return nil
	}

	var ds datastore.Datastore
	var err error
	if o.finalityCertsStorePath == "" {
		logger.Infow("Finality certificate store path is not set. Using in-memory datastore for finality certificates.")
		ds = sync2.MutexWrap(datastore.NewMapDatastore())
	} else {
		ds, err = leveldb.NewDatastore(path.Clean(o.finalityCertsStorePath), nil)
		if err != nil {
			return fmt.Errorf("failed to create datastore for finality certificates: %w", err)
		}
	}
	certsClient := certexchange.Client{
		RequestTimeout: o.finalityCertsClientRequestTimeout,
		Host:           o.host,
		NetworkName:    o.networkName,
	}

	var cs *certstore.Store
	if cs, err = certstore.OpenStore(ctx, ds); errors.Is(err, certstore.ErrNotInitialized) {
		table, err := certexchange.FindInitialPowerTable(ctx, certsClient, o.initialPowerTableCID, o.ecPeriod)
		if err != nil {
			return fmt.Errorf("failed to find initial power table: %w", err)
		}
		cs, err = certstore.CreateStore(ctx, ds, 0, table)
		if err != nil {
			return fmt.Errorf("failed to create certificate store: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to open certificate store: %w", err)
	}

	subscriber := polling.Subscriber{
		Client:              certsClient,
		Store:               cs,
		SignatureVerifier:   o.finalityCertsVerifier,
		InitialPollInterval: o.finalityCertsInitialPollInterval,
		MaximumPollInterval: o.finalityCertsMaxPollInterval,
		MinimumPollInterval: o.finalityCertsMinPollInterval,
	}
	if err := subscriber.Start(ctx); err != nil {
		return fmt.Errorf("failed to start finality certificate subscriber: %w", err)
	}

	certificates, unsubscribe := cs.Subscribe()
	defer func() {
		unsubscribe()
		if err := subscriber.Stop(context.Background()); err != nil {
			logger.Warnw("Failed to stop finality certificate subscriber", "err", err)
		}
		if err := ds.Close(); err != nil {
			logger.Warnw("Failed to close datastore for finality certificates", "err", err)
		}
	}()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return nil
		case cert := <-certificates:
			certificate, err := newFinalityCertificate(time.Now().UTC(), o.networkName, cert)
			if err != nil {
				logger.Errorw("Failed to convert finality certificate", "certificate", cert, "err", err)
				continue
			}
			if err := o.storeFinalityCertificate(ctx, certificate); err != nil {
				logger.Errorw("Failed to store finality certificate", "certificate", certificate, "err", err)
				continue
			}
			logger.Debugw("Observed finality certificate", "certificate", certificate)
		}
	}
	return nil
}

func (o *Observer) storeMessage(_ context.Context, om *Message) error {
	var justification any
	if om.Justification != nil {
		// Dereference to get the go-duckdb reflection in appender to behave. Otherwise,
		// it fails as it interprets the type as mismatch due to how nullable fields are
		// handled.
		justification = *om.Justification
	}
	if err := o.dbLatestMessages.AppendRow(
		om.Timestamp,
		om.NetworkName,
		int64(om.Sender),
		om.Vote,
		om.Signature,
		om.Ticket,
		justification,
		om.VoteValueKey,
	); err != nil {
		return fmt.Errorf("failed to append row: %w", err)
	}
	o.unflushedMessageCount++
	return o.tryFlushMessages()
}

func (o *Observer) storeFinalityCertificate(_ context.Context, oc *FinalityCertificate) error {
	if err := o.dbFinalityCertificates.AppendRow(
		oc.Timestamp,
		oc.NetworkName,
		oc.Instance,
		oc.ECChain,
		oc.SupplementalData,
		oc.Signers,
		oc.Signature,
		oc.PowerTableDelta,
	); err != nil {
		return fmt.Errorf("failed to append row: %w", err)
	}
	o.unflushedCertificateCount++
	return o.tryFlushCertificates()
}

func (o *Observer) tryFlushMessages() error {
	batchSizeReached := o.unflushedMessageCount >= o.maxBatchSize
	batchDelayElapsed := !o.lastFlushedMessagesAt.IsZero() && time.Since(o.lastFlushedMessagesAt) >= o.maxBatchDelay
	if batchSizeReached || batchDelayElapsed {
		if err := o.dbLatestMessages.Flush(); err != nil {
			return fmt.Errorf("failed to flush appender: %w", err)
		}
		logger.Infow("Flushed messages to database", "count", o.unflushedMessageCount, "after", time.Since(o.lastFlushedMessagesAt))
		o.unflushedMessageCount = 0
		o.lastFlushedMessagesAt = time.Now()
	}
	return nil
}

func (o *Observer) tryFlushCertificates() error {
	batchSizeReached := o.unflushedCertificateCount >= o.maxBatchSize
	batchDelayElapsed := !o.lastFlushedCertificatesAt.IsZero() && time.Since(o.lastFlushedCertificatesAt) >= o.maxBatchDelay
	if batchSizeReached || batchDelayElapsed {
		if err := o.dbFinalityCertificates.Flush(); err != nil {
			return fmt.Errorf("failed to flush appender: %w", err)
		}
		logger.Infow("Flushed finality certificates to database", "count", o.unflushedCertificateCount, "after", time.Since(o.lastFlushedCertificatesAt))
		o.unflushedCertificateCount = 0
		o.lastFlushedCertificatesAt = time.Now()
	}
	return nil
}

func (o *Observer) rotateMessages(ctx context.Context) error {
	output := filepath.Join(o.rotatePath, fmt.Sprintf("log_%s.parquet", time.Now().Format(time.RFC3339)))
	rotateQuery := fmt.Sprintf(`COPY latest_messages TO '%s' (FORMAT 'parquet', COMPRESSION 'zstd');`, output)
	if _, err := o.db.ExecContext(ctx, rotateQuery); err != nil {
		return fmt.Errorf("failed to export latest messages to parquet: %w", err)
	}
	if _, err := o.db.ExecContext(ctx, `TRUNCATE latest_messages;`); err != nil {
		return fmt.Errorf("failed to truncate latest_messages: %w", err)
	}
	logger.Infow("Rotated messages successfully.", "output", output)

	if stat, err := os.Stat(output); err != nil {
		logger.Errorw("Failed to stat log file", "err", err)
	} else {
		logger.Infow("Rotated messages to file", "file", stat.Name(), "size", stat.Size())
	}

	dir, err := os.ReadDir(o.rotatePath)
	if err != nil {
		return err
	}
	var foundAtLeastOneParquet bool
	for _, entry := range dir {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".parquet" {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if o.retention > 0 && info.ModTime().Before(time.Now().Add(-o.retention)) {
				if err := os.Remove(filepath.Join(o.rotatePath, entry.Name())); err != nil {
					logger.Errorw("Failed to remove retention policy for file", "file", entry.Name(), "err", err)
				} else {
					logger.Infow("Removed old file", "olderThan", o.retention, "file", entry.Name())
				}
			} else {
				foundAtLeastOneParquet = true
			}
		}
	}

	return o.createOrReplaceMessagesView(ctx, foundAtLeastOneParquet)
}

func (o *Observer) listenAndServeQueries() error {
	if err := o.qs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Errorw("Query server stopped erroneously", "err", err)
		return err
	}
	return nil
}

func (o *Observer) stayConnected(ctx context.Context) error {
	if o.connectivityCheckInterval <= 0 {
		logger.Info("Re-connectivity is disabled")
		return nil
	}
	ticker := time.NewTicker(o.connectivityCheckInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			o.repairConnectivity(ctx)
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (o *Observer) repairConnectivity(ctx context.Context) {
	connectivity := o.countConnectedPeers()
	o.tryConnectToBootstrapPeers(ctx)
	o.tryConnectToFilecoinDHT(ctx)
	o.tryConnectToLotusNetPeers(ctx)
	logger.Infow("Connectivity cycle completed", "before", connectivity, "after", o.countConnectedPeers())
}

func (o *Observer) countConnectedPeers() int {
	return len(o.host.Network().Peers())
}

func (o *Observer) tryConnectToLotusNetPeers(ctx context.Context) {
	if o.connectivityLotusPeersThreshold <= 0 {
		return
	}
	connectivity := o.countConnectedPeers()
	if connectivity >= o.connectivityLotusPeersThreshold {
		return
	}
	peers := lotus.ListAllPeers(ctx, o.connectivityLotusAPIEndpoints...)
	logger.Infow("Low network connectivity, reconnecting to peers discovered via Lotus", "connectivity", connectivity, "lotusPeers", len(peers), "threshold", o.connectivityLotusPeersThreshold)
	var eg errgroup.Group
	eg.SetLimit(o.connectivityConcurrency)
	for _, addr := range peers {
		select {
		case <-ctx.Done():
		default:
			eg.Go(func() error {
				if err := o.host.Connect(ctx, addr); err != nil {
					logger.Debugw("Failed to connect to peer discovered via Lotus", "peer", addr, "err", err)
				}
				return nil
			})
		}
	}
	_ = eg.Wait()
}

func (o *Observer) tryConnectToFilecoinDHT(ctx context.Context) {
	if o.connectivityDHTThreshold <= 0 {
		return
	}

	connectivity := o.countConnectedPeers()
	if connectivity < o.connectivityDHTThreshold {
		logger.Infow("Low network connectivity, re-bootstrapping via DHT", "connectivity", connectivity, "threshold", o.connectivityDHTThreshold)
		if err := o.dht.Bootstrap(ctx); err != nil {
			logger.Errorw("DHT bootstrap failed", "err", err)
		}
	}
}

func (o *Observer) tryConnectToBootstrapPeers(ctx context.Context) {
	if len(o.connectivityBootstrapPeers) == 0 {
		return
	}
	connectivity := o.countConnectedPeers()
	if connectivity >= o.connectivityBootstrappersThreshold {
		return
	}
	logger.Infow("Low network connectivity, re connecting to bootstrap peers", "connectivity", connectivity, "threshold", o.connectivityBootstrappersThreshold)
	var eg errgroup.Group
	eg.SetLimit(o.connectivityConcurrency)
	for _, addr := range o.connectivityBootstrapPeers {
		select {
		case <-ctx.Done():
		default:
			eg.Go(func() error {
				if err := o.host.Connect(ctx, addr); err != nil {
					logger.Debugw("Failed to connect to bootstrap peer", "peer", addr, "err", err)
				}
				return nil
			})
		}
	}
	_ = eg.Wait()
}

func (o *Observer) startMessageObserverFor(ctx context.Context, networkName gpbft.NetworkName) (_stop func(), _err error) {
	topicName := manifest.PubSubTopicFromNetworkName(networkName)
	var (
		topic        *pubsub.Topic
		subscription *pubsub.Subscription
		err          error
	)

	defer func() {
		if _err != nil {
			if !o.pubSubValidatorDisabled {
				_ = o.pubSub.UnregisterTopicValidator(topicName)
				if topic != nil {
					_ = topic.Close()
				}
			}
			if subscription != nil {
				subscription.Cancel()
			}
		}
	}()
	if !o.pubSubValidatorDisabled {
		if err := o.pubSub.RegisterTopicValidator(topicName, o.validatePubSubGMessage); err != nil {
			return nil, fmt.Errorf("failed to register topic validator: %w", err)
		}
	} else {
		logger.Debugw("Skipping Topic validator registry for topic", "topic", topicName)
	}
	topic, err = o.pubSub.Join(topicName, pubsub.WithTopicMessageIdFn(psutil.GPBFTMessageIdFn))
	if err != nil {
		return nil, fmt.Errorf("failed to join topic: %w", err)
	}
	if err = topic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		return nil, fmt.Errorf("failed to set topic score params: %w", err)
	}
	subscription, err = topic.Subscribe(pubsub.WithBufferSize(o.subBufferSize))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			subscription.Cancel()
			_ = topic.Close()
			wg.Done()
		}()

		for ctx.Err() == nil {
			msg, err := subscription.Next(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorw("Failed to get next pubsub message", "network", networkName, "err", err)
				continue
			}
			if msg == nil {
				continue
			}

			om, err := newMessage(time.Now().UTC(), string(networkName), msg.ValidatorData.(gpbft.PartialGMessage))
			if err != nil {
				logger.Errorw("Failed to instantiate observation message", "err", err)
				continue
			}

			select {
			case <-ctx.Done():
				return
			case o.messageObserved <- om:
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}, nil
}

func (o *Observer) startChainExchangeObserverFor(ctx context.Context, networkName gpbft.NetworkName) (_stop func(), _err error) {
	topicName := manifest.ChainExchangeTopicFromNetworkName(networkName)
	var (
		topic        *pubsub.Topic
		subscription *pubsub.Subscription
		err          error
	)

	defer func() {
		if _err != nil {
			if !o.pubSubValidatorDisabled {
				_ = o.pubSub.UnregisterTopicValidator(topicName)
				if topic != nil {
					_ = topic.Close()
				}
			}
			if subscription != nil {
				subscription.Cancel()
			}
		}
	}()
	if err := o.pubSub.RegisterTopicValidator(topicName, o.validatePubSubChainExchangeMessage); err != nil {
		return nil, fmt.Errorf("failed to register chain exchange topic validator: %w", err)
	}
	topic, err = o.pubSub.Join(topicName, pubsub.WithTopicMessageIdFn(psutil.ChainExchangeMessageIdFn))
	if err != nil {
		return nil, fmt.Errorf("failed to join topic: %w", err)
	}
	if err = topic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		logger.Warnw("failed to set topic score params for chain exchange", "err", err)
	}
	subscription, err = topic.Subscribe(pubsub.WithBufferSize(o.subBufferSize))
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to chain exchange topic: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			subscription.Cancel()
			_ = topic.Close()
			wg.Done()
		}()

		for ctx.Err() == nil {
			msg, err := subscription.Next(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorw("Failed to get next pubsub message from chain exchange topic", "network", networkName, "err", err)
				continue
			}
			if msg == nil || msg.ValidatorData == nil {
				continue
			}

			ceMsg, ok := msg.ValidatorData.(chainexchange.Message)
			if !ok {
				logger.Errorw("Received message with invalid ValidatorData type", "expected", "chainexchange.Message", "got", fmt.Sprintf("%T", msg.ValidatorData))
				continue
			}

			select {
			case <-ctx.Done():
				return
			case o.chainExchangeObserved <- &ceMsg:
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}, nil
}

func (o *Observer) anyParquetFilesPresent() (bool, error) {
	dir, err := os.ReadDir(o.rotatePath)
	if err != nil {
		return false, err
	}
	for _, entry := range dir {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".parquet" {
			return true, nil
		}
	}
	return false, nil
}

func (o *Observer) Stop(ctx context.Context) error {
	var err error
	err = multierr.Append(err, o.qs.Shutdown(ctx))
	if o.stop != nil {
		err = multierr.Append(err, o.stop())
	}
	return err
}

func (o *Observer) validatePubSubGMessage(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var pgmsg gpbft.PartialGMessage
	if err := o.gMsgEncoding.Decode(msg.Data, &pgmsg); err != nil {
		return pubsub.ValidationReject
	}
	msg.ValidatorData = pgmsg
	return pubsub.ValidationAccept
}

func (o *Observer) validatePubSubChainExchangeMessage(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var ceMsg chainexchange.Message
	if err := o.ceMsgEncoding.Decode(msg.Data, &ceMsg); err != nil {
		logger.Debugw("Failed to decode chain exchange message", "err", err, "message", msg)
		return pubsub.ValidationReject
	}
	if err := ceMsg.Chain.Validate(); err != nil {
		logger.Debugw("Invalid chain in chain exchange message", "err", err, "message", ceMsg)
		return pubsub.ValidationReject
	}
	if ceMsg.Chain.IsZero() {
		logger.Debugw("Chain in chain exchange message is zero", "message", ceMsg)
		return pubsub.ValidationReject
	}
	now := time.Now().UnixMilli()
	lowerBound := now - o.chainExchangeMaxMessageAge.Milliseconds()
	if lowerBound > ceMsg.Timestamp || ceMsg.Timestamp > now {
		logger.Debugw("Timestamp too old or too far ahead", "from", msg.GetFrom(), "timestamp", ceMsg.Timestamp, "lowerBound", lowerBound)
		return pubsub.ValidationIgnore
	}
	msg.ValidatorData = ceMsg
	return pubsub.ValidationAccept
}
