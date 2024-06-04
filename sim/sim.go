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
	hosts        []*simHost
	participants []Participant
	adversary    *adversary.Adversary
}

// Participant is a wrapper around gpbft.Participant that implements the Receiver interface
type Participant struct {
	id gpbft.ActorID
	*gpbft.Participant
}

func newParticipant(id gpbft.ActorID, host gpbft.Host, pOpts ...gpbft.Option) (Participant, error) {
	participant, err := gpbft.NewParticipant(host, pOpts...)
	if err != nil {
		return Participant{}, fmt.Errorf("failed to instantiate participant: %w", err)
	}
	return Participant{
		id:          id,
		Participant: participant,
	}, nil
}

func (p Participant) ID() gpbft.ActorID {
	return p.id
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
	pt, err := s.getPowerTable(0)
	if err != nil {
		return err
	}
	currentInstance := s.ec.BeginInstance(*s.baseChain, pt)
	s.startParticipants()

	finalInstance := instanceCount - 1

	// Exclude adversary ID when checking for decision or instance completion.
	if s.adversary != nil {
		s.ignoreConsensusFor = append(s.ignoreConsensusFor, s.adversary.ID())
	}

	// Run until there are no more messages, meaning termination or deadlock.
	moreTicks := true
	for moreTicks {
		if err := s.ec.Err(); err != nil {
			return fmt.Errorf("error in decision: %w", err)
		}
		if s.getMaxRound() > maxRounds {
			return fmt.Errorf("reached maximum number of %d rounds at instance %d", maxRounds, currentInstance.Instance)
		}
		if currentInstance.HasCompleted(s.ignoreConsensusFor...) {
			// Verify the current instance as soon as it completes.
			decidedChain, reachedConsensus := currentInstance.HasReachedConsensus(s.ignoreConsensusFor...)
			if !reachedConsensus {
				return fmt.Errorf("concensus was not reached at instance %d", currentInstance.Instance)
			}

			pt, err := s.getPowerTable(currentInstance.Instance + 1)
			if err != nil {
				return err
			}

			// Instantiate the next instance even if it goes beyond finalInstance.
			// The last incomplete instance is used for testing assertions.
			currentInstance = s.ec.BeginInstance(*decidedChain, pt)

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
		p.Start()
	}
	// Start adversary
	if s.adversary != nil {
		if err := s.adversary.Start(); err != nil {
			panic(fmt.Errorf("adversary %d failed starting: %w", s.adversary.ID(), err))
		}
	}
}

// Gets the power table to be used for an instance.
func (s *Simulation) getPowerTable(instance uint64) (*gpbft.PowerTable, error) {
	pEntries := make([]gpbft.PowerEntry, 0, len(s.participants))
	// Set chains for first instance
	for _, h := range s.hosts {
		pEntries = append(pEntries, gpbft.PowerEntry{
			ID:     h.ID(),
			Power:  h.StoragePower(instance),
			PubKey: h.PublicKey(instance),
		})
	}
	pt := gpbft.NewPowerTable()
	if err := pt.Add(pEntries...); err != nil {
		return nil, fmt.Errorf("failed to set up power table at first instance: %w", err)
	}
	return pt, nil
}

func (s *Simulation) initParticipants() error {
	pOpts := append(s.gpbftOptions, gpbft.WithTracer(s.network))
	var nextID gpbft.ActorID
	for _, archetype := range s.honestParticipantArchetypes {
		for i := 0; i < archetype.count; i++ {
			host := newHost(nextID, s, archetype.ecChainGenerator, archetype.storagePowerGenerator)
			participant, err := newParticipant(nextID, host, pOpts...)
			if err != nil {
				return err
			}
			s.participants = append(s.participants, participant)
			s.hosts = append(s.hosts, host)
			s.network.AddParticipant(nextID, participant)
			nextID++
		}
	}

	// There is at most one adversary but with arbitrary power.
	if s.adversaryGenerator != nil && s.adversaryCount == 1 {
		host := newHost(nextID, s, NewFixedECChainGenerator(*s.baseChain), nil)
		// Adversary implementations currently ignore the canonical chain.
		// Set to a fixed ec chain generator and expand later for possibility
		// of implementing adversaries that adapt based on ec chain.
		s.adversary = s.adversaryGenerator(nextID, host)
		// Adversary power does not evolve.
		host.spg = UniformStoragePower(s.adversary.Power)
		s.hosts = append(s.hosts, host)
		s.network.AddParticipant(nextID, s.adversary)
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

// ListParticipantIDs lists the ID of honest participants in simulation. Note
// that the adversary ID is not included in the list.
func (s *Simulation) ListParticipantIDs() []gpbft.ActorID {
	pids := make([]gpbft.ActorID, len(s.participants))
	for i, participant := range s.participants {
		pids[i] = participant.ID()
	}
	return pids
}

func (s *Simulation) GetInstance(i uint64) *ECInstance {
	return s.ec.GetInstance(i)
}

func (s *Simulation) getMaxRound() uint64 {
	var maxRound uint64
	for _, participant := range s.participants {
		currentRound := participant.CurrentRound()
		if currentRound > maxRound {
			maxRound = currentRound
		}
	}
	return maxRound
}
