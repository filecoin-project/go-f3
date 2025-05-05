package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/observer"
	"github.com/filecoin-project/go-f3/pmsg"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var aiderCmd = cli.Command{
	Name: "aid",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "observer",
			Usage: "The observer query endpoint.",
			Value: "https://mainnet-observer.f3.eng.filoz.org/query",
		},
		&cli.StringFlag{
			Name:    "observerAuth",
			Usage:   "The Authorization header value to use when calling the observer.",
			EnvVars: []string{"F3_OBSERVER_AUTH"},
		},
		&cli.StringFlag{
			Name:  "networkName",
			Usage: "The F3 network name.",
			Value: "filecoin",
		},
		&cli.IntFlag{
			Name:  "connLo",
			Usage: "The lower connection manager watermark.",
			Value: 160,
		},
		&cli.IntFlag{
			Name:  "connHi",
			Usage: "The higher connection manager watermark.",
			Value: 512,
		},
		&cli.StringSliceFlag{
			Name:  "lotusDaemon",
			Usage: "A lotus daemon API endpoint to use for peer discovery.",
		},
		&cli.DurationFlag{
			Name:  "reconnectInterval",
			Usage: "The interval to wait before reconnecting to all peers.",
			Value: 10 * time.Second,
		},
		&cli.IntFlag{
			Name:  "reconnectConcurrency",
			Usage: "The degree of concurrency to use when reconnecting to peers.",
			Value: 50,
		},
		&cli.DurationFlag{
			Name:  "aidInterval",
			Usage: "The interval to wait before attempting to aid the F3 network.",
			Value: 10 * time.Second,
		},
		&cli.IntFlag{
			Name:  "aidConcurrency",
			Usage: "The degree of concurrency to use when fetching and publishing messages.",
			Value: 50,
		},
	},

	Action: func(c *cli.Context) error {

		connMngr, err := connmgr.NewConnManager(c.Int("connLo"), c.Int("connHi"))
		if err != nil {
			return fmt.Errorf("failed to create connection manager: %w", err)
		}
		host, err := libp2p.New(libp2p.UserAgent("f3-aider"), libp2p.ConnectionManager(connMngr))
		if err != nil {
			return fmt.Errorf("failed to create libp2p host: %w", err)
		}
		defer func() { _ = host.Close() }()

		// settings are forked from lotus
		const (
			GossipScoreThreshold             = -500
			PublishScoreThreshold            = -1000
			GraylistScoreThreshold           = -2500
			AcceptPXScoreThreshold           = 1000
			OpportunisticGraftScoreThreshold = 3.5
		)
		ps, err := pubsub.NewGossipSub(c.Context, host,
			pubsub.WithPeerExchange(true),
			pubsub.WithFloodPublish(true),
			pubsub.WithMessageIdFn(psutil.GPBFTMessageIdFn),
			pubsub.WithPeerGater(pubsub.NewPeerGaterParams(
				0.33,
				pubsub.ScoreParameterDecay(2*time.Minute),
				pubsub.ScoreParameterDecay(time.Hour),
			)),
			pubsub.WithPeerScore(
				&pubsub.PeerScoreParams{
					AppSpecificScore: func(p peer.ID) float64 {

						// Promote the mainnet F3 observer to a higher score
						const mainnetF3Observer = `12D3KooWCkzAoQkRFTy64dNCbaoQ1anXEv6K8xyJpGpj8BYtJCb7`
						if p.String() == mainnetF3Observer {
							return 1500
						}
						return 0
					},
					AppSpecificWeight: 1,

					// This sets the IP colocation threshold to 5 peers before we apply penalties
					IPColocationFactorThreshold: 5,
					IPColocationFactorWeight:    -100,

					// P7: behavioural penalties, decay after 1hr
					BehaviourPenaltyThreshold: 6,
					BehaviourPenaltyWeight:    -10,
					BehaviourPenaltyDecay:     pubsub.ScoreParameterDecay(time.Hour),

					DecayInterval: pubsub.DefaultDecayInterval,
					DecayToZero:   pubsub.DefaultDecayToZero,

					// this retains non-positive scores for 6 hours
					RetainScore: 6 * time.Hour,
				},
				&pubsub.PeerScoreThresholds{
					GossipThreshold:             GossipScoreThreshold,
					PublishThreshold:            PublishScoreThreshold,
					GraylistThreshold:           GraylistScoreThreshold,
					AcceptPXThreshold:           AcceptPXScoreThreshold,
					OpportunisticGraftThreshold: OpportunisticGraftScoreThreshold,
				},
			),
		)
		if err != nil {
			return fmt.Errorf("failed to create pubsub subscriber: %w", err)
		}

		topicName := manifest.PubSubTopicFromNetworkName(gpbft.NetworkName(c.String("networkName")))
		f3Chatter, err := ps.Join(topicName, pubsub.WithTopicMessageIdFn(psutil.GPBFTMessageIdFn))
		if err != nil {
			return fmt.Errorf("failed to join pubsub topic %s: %w", topicName, err)
		}

		connectToAll := func() {
			var eg errgroup.Group
			eg.SetLimit(c.Int("reconnectConcurrency"))
			peers := lotusNetPeers(c)
			var count atomic.Int32
			for _, peer := range peers {
				select {
				case <-c.Context.Done():
				default:
					eg.Go(func() error {
						if err := host.Connect(c.Context, peer); err == nil {
							count.Add(1)
						}
						return nil
					})
				}
			}
			_ = eg.Wait()
			fmt.Printf("🔗 Connected to %d peers\n", count.Load())
		}

		// Connect to all before doing anything.
		fmt.Println("Connecting to all peers before starting the aid cycle...")
		connectToAll()

		go func() {
			ticker := time.NewTicker(c.Duration("aidInterval"))
			defer ticker.Stop()
			for c.Context.Err() == nil {
				select {
				case <-c.Context.Done():
					return
				case <-ticker.C:
					if err := aid(c, f3Chatter); err != nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Errorf("❌ failed to aid: %w", err))
					}
				}
			}
		}()

		// periodically reconnect to all.
		go func() {
			ticker := time.NewTicker(c.Duration("reconnectInterval"))
			defer ticker.Stop()
			for c.Context.Err() == nil {
				select {
				case <-c.Context.Done():
					return
				case <-ticker.C:
					connectToAll()
				}
			}
		}()

		<-c.Context.Done()
		return nil
	},
}

func aid(c *cli.Context, f3Chatter *pubsub.Topic) error {
	instance, round, err := getLatestInstanceRound(c)
	if err != nil {
		return err
	}
	fmt.Printf("📋 Latest instance %d at round %d\n", instance, round)
	senders, err := listDistinctSendersByInstance(c, instance)
	if err != nil {
		return err
	}
	fmt.Printf("⚙️ Found %d distinct senders\n", len(senders))

	msgEncoding, err := encoding.NewZSTD[*pmsg.PartialGMessage]()
	if err != nil {
		return fmt.Errorf("failed to create zstd message encoding: %w", err)
	}

	var messagesCount atomic.Int32
	var sendersCount atomic.Int32

	aidSender := func(sender uint64) {
		msgs, err := listBroadcastMessagesByInstanceRoundSender(c, instance, round, sender)
		if err != nil {
			err := fmt.Errorf("failed to list broadcast messages from sender %d: %w", sender, err)
			_, _ = fmt.Fprintln(os.Stderr, err)
			return
		}
		for _, msg := range msgs {
			partial, err := msg.ToPartialMessage()
			if err != nil {
				err := fmt.Errorf("failed to construct partial message for sender %d: %w", sender, err)
				_, _ = fmt.Fprintln(os.Stderr, err)
				return
			}
			encoded, err := msgEncoding.Encode(partial)
			if err != nil {
				err := fmt.Errorf("failed to encode mesage for sender %d: %w", sender, err)
				_, _ = fmt.Fprintln(os.Stderr, err)
				return
			}
			err = f3Chatter.Publish(c.Context, encoded)
			if err != nil {
				err := fmt.Errorf("failed to rebroadcast message forom sender %d: %w", sender, err)
				_, _ = fmt.Fprintln(os.Stderr, err)
				return
			}
			messagesCount.Add(1)
		}

	}

	var eg errgroup.Group

	eg.SetLimit(c.Int("aidConcurrency"))
	for _, sender := range senders {
		select {
		case <-c.Context.Done():
		default:
			eg.Go(func() error {
				aidSender(sender)
				sendersCount.Add(1)
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to aid: %w", err)
	}
	fmt.Printf("🔊 Rebroadcasted %d messages from senders %d\n", messagesCount.Load(), sendersCount.Load())
	return nil
}

func listBroadcastMessagesByInstanceRoundSender(c *cli.Context, instance, round, sender uint64) ([]observer.Message, error) {
	var messages []struct {
		DedupedMessage observer.Message `json:"msg"`
	}
	if err := query(c, fmt.Sprintf(`
SELECT Sender, Vote.Round, Vote.Instance, Vote.Phase, ARBITRARY(m) AS msg
FROM Messages AS m
WHERE Vote.Instance = %d
  AND Sender = %d
  AND (Vote.Round > %d OR Vote.Phase = 'QUALITY')
GROUP BY Sender, Vote.Round, Vote.Instance, Vote.Phase;
`, instance, sender, max(int(round)-2, 0)), &messages); err != nil {
		return nil, err
	}

	msgs := make([]observer.Message, len(messages))
	for i, message := range messages {
		msgs[i] = message.DedupedMessage
	}
	return msgs, nil

}
func listDistinctSendersByInstance(c *cli.Context, instance uint64) ([]uint64, error) {
	var senders []struct {
		Sender uint64 `json:"sender"`
	}
	if err := query(c, fmt.Sprintf(`
SELECT DISTINCT Sender as sender
FROM latest_messages
WHERE (Vote).Instance = %d
ORDER BY Sender;
`, instance), &senders); err != nil {
		return nil, err
	}
	if len(senders) == 0 {
		return nil, nil
	}
	ids := make([]uint64, 0, len(senders))
	for _, sender := range senders {
		ids = append(ids, sender.Sender)
	}
	return ids, nil
}

func getLatestInstanceRound(c *cli.Context) (uint64, uint64, error) {
	var latest []struct {
		Instance uint64 `json:"instance"`
		Round    uint64 `json:"round"`
	}
	if err := query(c, `
SELECT (Vote).Instance As instance, MAX((Vote).Round) AS round
FROM latest_messages
WHERE (Vote).Instance = (SELECT MAX((Vote).Instance) FROM latest_messages)
GROUP BY (Vote).Instance;
`, &latest); err != nil {
		return 0, 0, err
	}
	if len(latest) == 0 {
		return 0, 0, fmt.Errorf("no latest instance found")
	}
	return latest[0].Instance, latest[0].Round, nil
}

func query[R any](c *cli.Context, query string, result R) error {
	url := c.String("observer")
	body := bytes.NewReader([]byte(query))
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", `text/plain`)
	req.Header.Set("Authorization", c.String("observerAuth"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("failed to unmarshal response: %s: %w", string(respBody), err)
	}
	return nil
}

func lotusNetPeers(c *cli.Context) []peer.AddrInfo {
	lotusDaemons := c.StringSlice("lotusDaemon")
	if len(lotusDaemons) == 0 {
		return nil
	}

	const netPeersJsonRpc = `{"method":"Filecoin.NetPeers","params":[],"id":2,"jsonrpc":"2.0"}`

	type resultOrError struct {
		Result []peer.AddrInfo `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		}
	}

	var addrs []peer.AddrInfo
	seen := make(map[string]struct{})
	for _, endpoint := range lotusDaemons {
		body := bytes.NewReader([]byte(netPeersJsonRpc))
		req, err := http.NewRequest("POST", endpoint, body)
		if err != nil {
			err := fmt.Errorf("failed construct request to discover peers from: %s :%w", endpoint, err)
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}
		req.Header.Set("Content-Type", `application/json`)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			err := fmt.Errorf("failed to discover peers from lotus daemon %s: %w", endpoint, err)
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			err := fmt.Errorf("failed to read response body from lotus daemon %s: %w", endpoint, err)
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}
		var roe resultOrError
		if err := json.Unmarshal(respBody, &roe); err != nil {
			err := fmt.Errorf("failed to unmarshal response from lotus daemon %s: %s: %w", endpoint, string(respBody), err)
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}
		if roe.Error != nil {
			err := fmt.Errorf("failed to discover peers from lotus daemon %s: %s", endpoint, roe.Error.Message)
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}

		for _, addr := range roe.Result {
			k := addr.ID.String()
			if _, found := seen[k]; !found {
				addrs = append(addrs, addr)
				seen[k] = struct{}{}
			}
		}
	}
	return addrs
}
