package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/internal/encoding"
	"github.com/filecoin-project/go-f3/internal/lotus"
	"github.com/filecoin-project/go-f3/internal/psutil"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/go-f3/observer"
	"github.com/filecoin-project/go-f3/pmsg"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
		&cli.BoolFlag{
			Name:  "disableLagAwareness",
			Usage: "The option to disable aider lag awareness, where rebroadcast will be aided even if F3 does not seem to be struggling to reach a decision.",
		},
		&cli.Int64Flag{
			Name:        "genesisEpoch",
			Usage:       "The genesis epoch of the network in Unix time.",
			DefaultText: "Filecoin Mainnet genesis epoch",
			Value:       1598306400,
		},
		&cli.IntFlag{
			Name:  "maxEpochsFromHead",
			Usage: "The maximum number of epochs from the head of chain to consider for rebroadcasting.",
			Value: 6,
		},
		&cli.DurationFlag{
			Name:  "maxF3InstanceAge",
			Usage: "The maximum time to wait for an F3 instance before considering it as lagging.",
			Value: 3 * time.Minute,
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

		ps, err := pubsub.NewGossipSub(c.Context, host,
			pubsub.WithPeerExchange(true),
			pubsub.WithFloodPublish(true),
			pubsub.WithMessageIdFn(psutil.GPBFTMessageIdFn),
			pubsub.WithPeerGater(pubsub.NewPeerGaterParams(
				0.33,
				pubsub.ScoreParameterDecay(2*time.Minute),
				pubsub.ScoreParameterDecay(time.Hour),
			)),
			pubsub.WithPeerScore(psutil.PubsubPeerScoreParams, psutil.PubsubPeerScoreThresholds),
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
			peers := lotus.ListAllPeers(c.Context, c.StringSlice("lotusDaemon")...)
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
		fmt.Println("🎬 Connecting to all peers before starting the aid cycle...")
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

	var attemptAid bool
	var progress *gpbft.InstanceProgress
	if c.Bool("disableLagAwareness") {
		attemptAid = true
		var err error
		progress, err = getF3ProgressFromObserver(c)
		if err != nil {
			return err
		}
	} else {
		attemptAid, progress = isF3Lagging(c)
	}

	if !attemptAid && progress != nil {
		fmt.Printf("👌 Network does not seem to be lagging. Current progress: %v\n", progress)
		return nil
	}
	if progress == nil {
		// Sanity check.
		fmt.Println("❌ No progress information available. Skipping aid.")
		return nil
	}
	fmt.Printf("🩺 Attempting aid with F3 progress: %v\n", progress)

	instance := progress.Instant.ID
	round := progress.Instant.Round

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

var (
	latestLag          *gpbft.InstanceProgress
	latestLagChangedAt time.Time
)

func isF3Lagging(c *cli.Context) (_ bool, _lag *gpbft.InstanceProgress) {
	defer func() {
		if _lag != nil {
			// Only override the previous lag if there's new non-nil lag info.
			latestLag = _lag
			if latestLag != nil && compareProgress(*latestLag, *_lag) != 0 {
				// Only update the timestamp if the lag info has changed.
				latestLagChangedAt = time.Now()
			}
		}
	}()
	const epochSeconds = 30

	progresses := lotus.GetF3Progress(c.Context, c.StringSlice("lotusDaemon")...)
	if len(progresses) == 0 {
		// Either no lotus demons configured or error in getting f3 progress. Fall back
		// on getting progress from observer.
		progress, err := getF3ProgressFromObserver(c)
		if err != nil {
			log.Debugw("Failed to get latest instance round using observer", "err", err)

			if latestLag != nil {
				fmt.Printf("⚠️ Not enough information to determine lag. Defensively aiding rebroadcast using last known progress: %v\n", latestLag)
				return true, latestLag
			}
			fmt.Println("❌ Not enough information to determine lag nor any previous progress. Skipped aiding rebroadcast.")
			return false, nil
		}

		// We have the latest seen instance and round from observer. Treat F3 as lagging
		// if round is non-zero.
		return progress.Round > 0, progress
	}

	// There's live f3 progress fetched from lotus daemon. Conservatively pick the
	// least progress and use it to determine if F3 is lagging.
	slices.SortFunc(progresses, compareProgress)
	leastProgress := progresses[0]

	if leastProgress.Round > 0 {
		// Non-zero round; treat F3 as behind, since ideally all instances should finish
		// in a single round. This case will also cover CONVERGE, since it never happens
		// in round 0.
		return true, &leastProgress
	}
	if !leastProgress.Input.IsZero() {
		headEpoch := (time.Now().Unix() - c.Int64("genesisEpoch")) / epochSeconds
		latestFinalizableEpoch := leastProgress.Input.Head().Epoch
		distanceFromHead := headEpoch - latestFinalizableEpoch
		threshold := int64(c.Int("maxEpochsFromHead"))
		if distanceFromHead > threshold {
			// The latest finalizable epoch is too far behind the head epoch. Treat F3 as
			// lagging.
			fmt.Printf("🐌 F3 is too far behind head: %d > %d\n", distanceFromHead, threshold)
			return true, &leastProgress
		}
	}
	if latestLag != nil {
		previousID := latestLag.Instant.ID
		currentID := leastProgress.Instant.ID
		sinceLastChanged := time.Since(latestLagChangedAt)
		threshold := c.Duration("maxF3InstanceAge")
		if previousID == currentID && !latestLagChangedAt.IsZero() && sinceLastChanged > threshold {
			// Too much time has passed, and the instance is still the same. Treat F3 as
			// lagging.
			fmt.Printf("⏳F3 instance is taking too long to terminate: %v > %v\n", sinceLastChanged, threshold)
			return true, &leastProgress
		}
	}
	return false, &leastProgress
}

// compareProgress compares two InstanceProgress instances and returns an integer
// indicating their relative order. It prioritizes instances that are "further
// behind" based on the following criteria:
//  1. Instance ID: Smaller Instance IDs are considered further behind.
//  2. Round: Smaller Rounds are considered further behind.
//  3. Phase: Smaller Phases are considered further behind.
//  4. Input Length: Shorter Input lengths are considered further behind.
//
// Returns:
//   - 0 if the instances are equal.
//   - -1 if 'one' is further behind than 'other'.
//   - 1 if 'one' is further ahead than 'other'.
func compareProgress(one, other gpbft.InstanceProgress) int {
	if one.Instant.ID != other.Instant.ID {
		if one.Instant.ID < other.Instant.ID {
			return -1
		}
		return 1
	}
	if one.Instant.Round != other.Instant.Round {
		if one.Instant.Round < other.Instant.Round {
			return -1
		}
		return 1
	}
	if one.Instant.Phase != other.Instant.Phase {
		if one.Instant.Phase < other.Instant.Phase {
			return -1
		}
		return 1
	}
	if !one.Input.Eq(other.Input) {
		// We could check prefix or suffix here, but we don't care about that. Keeping it
		// simple by inferring the shorter chain as further behind.
		if one.Input.Len() < other.Input.Len() {
			return -1
		}
		return 1
	}
	return 0
}

func listBroadcastMessagesByInstanceRoundSender(c *cli.Context, instance, round, sender uint64) ([]observer.Message, error) {
	var messages []struct {
		DedupedMessage observer.Message `json:"msg"`
	}
	if err := queryObserver(c, fmt.Sprintf(`
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
	if err := queryObserver(c, fmt.Sprintf(`
SELECT DISTINCT Sender as sender
FROM Messages
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

func getF3ProgressFromObserver(c *cli.Context) (*gpbft.InstanceProgress, error) {
	var latest []struct {
		Instance uint64 `json:"instance"`
		Round    uint64 `json:"round"`
	}
	if err := queryObserver(c, `
SELECT (Vote).Instance As instance, MAX((Vote).Round) AS round
FROM Messages
WHERE (Vote).Instance = (SELECT MAX((Vote).Instance) FROM Messages)
GROUP BY (Vote).Instance;
`, &latest); err != nil {
		return nil, err
	}
	if len(latest) == 0 {
		return nil, fmt.Errorf("no latest instance found")
	}
	return &gpbft.InstanceProgress{
		Instant: gpbft.Instant{
			ID:    latest[0].Instance,
			Round: latest[0].Round,
			// Although it's possible to determine the latest seen phase from the observer,
			// fill it in with a default value. It doesn't make a difference in deciding
			// whether F3 is lagging or not, nor the aid process.
			Phase: gpbft.INITIAL_PHASE,
		},

		// TODO: fill this in from observer once we start recording chainexchnge
		//       messages, since we can know the vote value key.
		//       See: https://github.com/filecoin-project/go-f3/issues/970
		Input: nil,
	}, nil
}

func queryObserver[R any](c *cli.Context, query string, result R) error {
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
