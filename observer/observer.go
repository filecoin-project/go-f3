package observer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/filecoin-project/go-f3/internal/lotus"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/pmsg"
	"github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/marcboeker/go-duckdb"
	"go.uber.org/multierr"
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
	msgEncoding     *encoding.ZSTD[*pmsg.PartialGMessage]

	dbConnector           *duckdb.Connector
	dbAppender            *duckdb.Appender
	dbConnection          driver.Conn
	unflushedMessageCount int
	lastFlushedAt         time.Time
}

func New(o ...Option) (*Observer, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	msgEncoding, err := encoding.NewZSTD[*pmsg.PartialGMessage]()
	if err != nil {
		return nil, err
	}
	return &Observer{
		options:         opts,
		messageObserved: make(chan *Message, opts.messageBufferSize),
		msgEncoding:     msgEncoding,
	}, nil
}

func (o *Observer) Start(ctx context.Context) error {
	if err := o.initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialise observer: %w", err)
	}

	ctx, stop := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return o.observe(ctx) })
	eg.Go(func() error { return o.stayConnected(ctx) })
	eg.Go(o.listenAndServeQueries)
	o.stop = func() error {
		stop()
		return multierr.Combine(
			o.dbAppender.Close(),
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
	o.dbAppender, err = duckdb.NewAppenderFromConn(o.dbConnection, "", "latest_messages")
	if err != nil {
		return fmt.Errorf("failed to create duckdb appender: %w", err)
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

func (o *Observer) observe(ctx context.Context) error {
	rotation := time.NewTicker(o.rotateInterval)
	flush := time.NewTicker(o.maxBatchDelay)
	stopObserverForNetwork, err := o.startObserverFor(ctx, o.networkName)
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

func (o *Observer) storeMessage(_ context.Context, om *Message) error {
	var justification any
	if om.Justification != nil {
		// Dereference to get the go-duckdb reflection in appender to behave. Otherwise,
		// it fails as it interprets the type as mismatch due to how nullable fields are
		// handled.
		justification = *om.Justification
	}
	if err := o.dbAppender.AppendRow(
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

func (o *Observer) tryFlushMessages() error {
	batchSizeReached := o.unflushedMessageCount >= o.maxBatchSize
	batchDelayElapsed := !o.lastFlushedAt.IsZero() && time.Since(o.lastFlushedAt) >= o.maxBatchDelay
	if batchSizeReached || batchDelayElapsed {
		if err := o.dbAppender.Flush(); err != nil {
			return fmt.Errorf("failed to flush appender: %w", err)
		}
		logger.Infow("Flushed messages to database", "count", o.unflushedMessageCount, "after", time.Since(o.lastFlushedAt))
		o.unflushedMessageCount = 0
		o.lastFlushedAt = time.Now()
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

func (o *Observer) startObserverFor(ctx context.Context, networkName gpbft.NetworkName) (_stop func(), _err error) {
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
		if err := o.pubSub.RegisterTopicValidator(topicName, o.validatePubSubMessage); err != nil {
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

			om, err := newMessage(time.Now().UTC(), string(networkName), msg.ValidatorData.(pmsg.PartialGMessage))
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

func (o *Observer) validatePubSubMessage(_ context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var pgmsg pmsg.PartialGMessage
	if err := o.msgEncoding.Decode(msg.Data, &pgmsg); err != nil {
		return pubsub.ValidationReject
	}
	msg.ValidatorData = pgmsg
	return pubsub.ValidationAccept
}
