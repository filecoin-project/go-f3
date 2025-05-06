package observer

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/pmsg"
	"github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	_ "github.com/marcboeker/go-duckdb"
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
	stop   func() error
	pubSub *pubsub.PubSub
	db     *sql.DB
	qs     http.Server

	messageObserved chan *message
	networkChanged  <-chan gpbft.NetworkName
	msgEncoding     *encoding.ZSTD[*pmsg.PartialGMessage]
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
		messageObserved: make(chan *message, opts.messageBufferSize),
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
		return eg.Wait()
	}
	return nil
}

func (o *Observer) initialize(ctx context.Context) error {
	var err error
	// Set up pubsub to listen for GPBFT messages.
	if o.pubSub, err = pubsub.NewGossipSub(ctx, o.host,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true),
		pubsub.WithMessageIdFn(psutil.GPBFTMessageIdFn),
		pubsub.WithPeerScore(psutil.PubsubPeerScoreParams, psutil.PubsubPeerScoreThresholds),
	); err != nil {
		return fmt.Errorf("failed to initialise pubsub: %w", err)
	}

	// Set up network name change listener.
	if o.networkChanged, err = o.networkNameChangeListener(ctx, o.pubSub); err != nil {
		return err
	}

	// Set up database connection.
	if o.db, err = sql.Open("duckdb", o.dataSourceName); err != nil {
		return err
	}

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

	// Connect to bootstrap peers on initialisation to avoid the initial wait for
	// reconnect ticker.
	if err := o.connectToBootstrapPeers(ctx); err != nil {
		// Warn but not return the error. Because, the observer will reconnect
		// periodically if connectivity drops below options.connectivity.minPeers.
		//
		// See stayConnected.
		logger.Warnw("Unsuccessful initial connection to bootstrap peers", "err", err)
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
	rotation := time.NewTimer(o.rotateInterval)
	var stopObserverForNetwork func()

	defer func() {
		rotation.Stop()
		if stopObserverForNetwork != nil {
			stopObserverForNetwork()
		}
	}()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return nil
		case network, ok := <-o.networkChanged:
			if !ok {
				panic("network name change listener channel closed unexpectedly")
			}
			if stopObserverForNetwork != nil {
				stopObserverForNetwork()
			}
			var err error
			stopObserverForNetwork, err = o.startObserverFor(ctx, network)
			if err != nil {
				logger.Errorw("Failed to start observer for network", "network", network, "err", err)
			}
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
		}
	}
	return nil
}

func (o *Observer) storeMessage(ctx context.Context, om *message) error {
	const insertMessage = `INSERT INTO latest_messages VALUES(?,?,?,?::json,?,?,?::json,?);`
	voteMarshaled, err := json.Marshal(om.Vote)
	if err != nil {
		return fmt.Errorf("failed to marshal vote: %w", err)
	}
	var justificationMarshaled any
	if om.Justification != nil {
		v, err := json.Marshal(om.Justification)
		if err != nil {
			return fmt.Errorf("failed to marshal justification: %w", err)
		}
		justificationMarshaled = string(v)
	}
	if _, err := o.db.ExecContext(ctx, insertMessage,
		om.Timestamp,
		om.NetworkName,
		om.Sender,
		string(voteMarshaled),
		om.Signature,
		om.Ticket,
		justificationMarshaled,
		om.VoteValueKey,
	); err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
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
	ticker := time.NewTicker(o.connectivityCheckInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			if peers := len(o.host.Network().Peers()); peers < o.connectivityMinPeers {
				logger.Infow("Low network connectivity, re-bootstrapping", "peers", peers)
				if err := o.connectToBootstrapPeers(ctx); err != nil {
					logger.Errorw("Failed to reconnect with at least one bootstrapper", "err", err)
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (o *Observer) connectToBootstrapPeers(ctx context.Context) error {
	var err error
	for _, addr := range o.bootstrapAddrs {
		if ctx.Err() != nil {
			break
		}
		if cErr := o.host.Connect(ctx, addr); cErr != nil {
			err = multierr.Append(err, fmt.Errorf("failed to connect to bootstrap address: %v", addr))
		}
	}
	return err
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
			_ = o.pubSub.UnregisterTopicValidator(topicName)
			if topic != nil {
				_ = topic.Close()
			}
			if subscription != nil {
				subscription.Cancel()
			}
		}
	}()
	if err := o.pubSub.RegisterTopicValidator(topicName, o.validatePubSubMessage); err != nil {
		return nil, fmt.Errorf("failed to register topic validator: %w", err)
	}
	topic, err = o.pubSub.Join(topicName, pubsub.WithTopicMessageIdFn(psutil.GPBFTMessageIdFn))
	if err != nil {
		return nil, fmt.Errorf("failed to join topic: %w", err)
	}
	if err = topic.SetScoreParams(psutil.PubsubTopicScoreParams); err != nil {
		return nil, fmt.Errorf("failed to set topic score params: %w", err)
	}
	subscription, err = topic.Subscribe()
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
			_ = o.pubSub.UnregisterTopicValidator(topicName)
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
