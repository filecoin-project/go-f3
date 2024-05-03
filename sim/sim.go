package sim

import (
	"fmt"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
)

type Simulation struct {
	*options
	network      *Network
	ec           *simEC
	participants []*gpbft.Participant
	adversary    *adversary.Adversary
}

func NewSimulation(o ...Option) (*Simulation, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &Simulation{
		options: opts,
		network: newNetwork(opts),
		ec:      newEC(opts),
	}, nil
}

// Run runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(instanceCount uint64, maxRounds uint64) error {
	if err := s.initParticipants(); err != nil {
		return err
	}
	pt, err := s.initPowerTable()
	if err != nil {
		return err
	}

	currentInstance := s.ec.BeginInstance(*s.baseChain, pt, s.beacon)
	s.startParticipants()

	finalInstance := instanceCount - 1

	// Exclude adversary ID when checking for decision or instance completion.
	var excludedParticipants []gpbft.ActorID
	if s.adversary != nil {
		excludedParticipants = append(excludedParticipants, s.adversary.ID())
	}

	// Run until there are no more messages, meaning termination or deadlock.
	moreTicks := true
	for moreTicks {
		if err := s.ec.Err(); err != nil {
			return fmt.Errorf("error in decision: %w", err)
		}
		if s.participants[0].CurrentRound() >= maxRounds {
			return fmt.Errorf("reached maximum number of %d rounds", maxRounds)
		}
		if currentInstance.HasCompleted(excludedParticipants...) {
			// Verify the current instance as soon as it completes.
			decidedChain, reachedConsensus := currentInstance.HasReachedConsensus(excludedParticipants...)
			if !reachedConsensus {
				return fmt.Errorf("concensus was not reached at instance %d", currentInstance.Instance)
			}

			// Instantiate the next instance even if it goes beyond finalInstance.
			// The last incomplete instance is used for testing assertions.
			currentInstance = s.ec.BeginInstance(*decidedChain,
				// Copy the previous instance power table.
				// The simulator doesn't have any facility to evolve the power table.
				// See https://github.com/filecoin-project/go-f3/issues/114.
				currentInstance.PowerTable.Copy(),
				[]byte(fmt.Sprintf("beacon %d", currentInstance.Instance+1)),
			)

			// Stop after currentInstance is larger than finalInstance, which means we will
			// instantiate one extra instance that will not complete.
			if currentInstance.Instance > finalInstance {
				break
			}
		}
		var err error
		moreTicks, err = s.network.Tick(s.adversary)
		if err != nil {
			return fmt.Errorf("error performing simulation step: %w", err)
		}
	}
	return nil
}

func (s *Simulation) startParticipants() {
	// Start participants.
	for _, p := range s.participants {
		if err := p.Start(); err != nil {
			panic(fmt.Errorf("participant %d failed starting: %w", p.ID(), err))
		}
	}
	// Start adversary
	if s.adversary != nil {
		if err := s.adversary.Start(); err != nil {
			panic(fmt.Errorf("adversary %d failed starting: %w", s.adversary.ID(), err))
		}
	}
}

func (s *Simulation) initPowerTable() (*gpbft.PowerTable, error) {
	pEntries := make([]gpbft.PowerEntry, 0, len(s.participants))
	// Set chains for first instance
	for _, p := range s.participants {
		pubKey, _ := s.signingBacked.GenerateKey()
		pEntries = append(pEntries, gpbft.PowerEntry{
			ID: p.ID(),
			// TODO: support varying power distribution across participants.
			//       See: https://github.com/filecoin-project/go-f3/issues/114.
			Power:  gpbft.NewStoragePower(1),
			PubKey: pubKey,
		})
	}
	pt := gpbft.NewPowerTable()
	if err := pt.Add(pEntries...); err != nil {
		return nil, fmt.Errorf("failed to set up power table at first instance: %w", err)
	}
	if s.adversary != nil {
		aPubKey, _ := s.signingBacked.GenerateKey()
		err := pt.Add(gpbft.PowerEntry{
			ID:     s.adversary.ID(),
			Power:  s.adversary.Power,
			PubKey: aPubKey,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to set up adversary power entry at first instance: %w", err)
		}
	}
	return pt, nil
}

func (s *Simulation) initParticipants() error {
	pOpts := append(s.gpbftOptions, gpbft.WithTracer(s.network))
	var nextID gpbft.ActorID
	for _, archetype := range s.honestParticipantArchetypes {
		for i := 0; i < archetype.count; i++ {
			host := newHost(nextID, s, archetype.ecChainGenerator)
			participant, err := gpbft.NewParticipant(nextID, host, pOpts...)
			if err != nil {
				return fmt.Errorf("failed to instnatiate participant: %w", err)
			}
			s.participants = append(s.participants, participant)
			s.network.AddParticipant(participant)
			nextID++
		}
	}

	// TODO: expand the simulation to accommodate more than one adversary for
	//       a more realistic simulation.
	// Limit adversaryCount to exactly one for now to reduce LOC up for review.
	// Future PRs will expand this to support a group of adversaries.
	if s.adversaryGenerator != nil && s.adversaryCount == 1 {
		// Adversary implementations currently ignore the canonical chain.
		// Set to a fixed ec chain generator and expand later for possibility
		// of implementing adversaries that adapt based on ec chain.
		// TODO: expand adversary archetypes.
		host := newHost(nextID, s, NewFixedECChainGenerator(*s.baseChain))
		s.adversary = s.adversaryGenerator(nextID, host)
		s.network.AddParticipant(s.adversary)
	}
	return nil
}

func (s *Simulation) Describe() string {
	b := strings.Builder{}
	for _, p := range s.participants {
		b.WriteString(p.Describe())
		b.WriteString("\n")
	}
	return b.String()
}

func (s *Simulation) ListParticipantIDs() []gpbft.ActorID {
	return s.network.participantIDs
}

func (s *Simulation) GetInstance(i uint64) *ECInstance {
	return s.ec.GetInstance(i)
}
