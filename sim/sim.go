package sim

import (
	"errors"
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
	initialInstance := uint64(0)
	pt, err := s.getPowerTable(initialInstance)
	if err != nil {
		return err
	}
	currentInstance := s.ec.BeginInstance(*s.baseChain, pt)
	s.startParticipants(initialInstance)

	finalInstance := initialInstance + instanceCount - 1

	// Exclude adversary ID when checking for decision or instance completion.
	if s.adversary != nil {
		s.ignoreConsensusFor = append(s.ignoreConsensusFor, s.adversary.ID())
	}

	// Run until there are no more messages, meaning termination or deadlock.
	for s.network.HasMoreTicks() {
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

			// Simulate the behaviour of an honest integrator, where upon receiving finality
			// certificates from future rounds, i.e. the instance that just completed in
			// simulation, it signals the participant to skip ahead.
			for _, p := range s.participants {
				if instance, _, _ := p.Progress(); instance < currentInstance.Instance {
					// TODO: enhance control over propagation of finality certificates
					//       See: https://github.com/filecoin-project/go-f3/issues/327
					if err := p.StartInstanceAt(currentInstance.Instance, s.network.Time()); err != nil {
						return fmt.Errorf("participant %d failed to skip to instace %d: %w", p.ID(), currentInstance.Instance, err)
					}
				}
			}

			// Check if the next instance exists already as a participant might have started
			// the next instance early. This can happen in scenarios where:
			//  * justifications for the termination of an instance reaches some nodes before
			//    others due to partial connectivity, e.g. Deny or Drop adversaries, and
			//  * the simulation stabilisation delay is not long enough to halt the start of
			//    next instance for those nodes before others have caught up.
			//
			// See gpbft.ProposalProvider.
			currentInstance = s.ec.GetInstance(currentInstance.Instance + 1)
			if currentInstance == nil {
				// Instantiate the next instance even if it goes beyond finalInstance.
				// The last incomplete instance is used for testing assertions.
				currentInstance = s.ec.BeginInstance(*decidedChain, pt)
			} else if !currentInstance.BaseChain.Eq(*decidedChain) {
				// Assert that the instance that has already started uses the same base chain as
				// the one consistently decided among participants.
				return fmt.Errorf("network is partitioned")
			}

			// Stop after currentInstance is larger than finalInstance, which means we will
			// instantiate one extra instance that will not complete.
			if currentInstance.Instance > finalInstance {
				break
			}
		}

		switch err := s.network.Tick(s.adversary); {
		case errors.Is(err, gpbft.ErrValidationNotRelevant):
			// Ignore error signalling valid messages that are no longer useful for the
			// progress of GPBFT. This can occur in normal operation depending on the order
			// of delivered messages. In production, deployment this error is used to signal
			// that the message does not need to be propagated among participants. In
			// simulation, we simply ignore it.
		case err != nil:
			return fmt.Errorf("error performing simulation phase: %w", err)
		}
	}
	return nil
}

func (s *Simulation) startParticipants(instance uint64) {
	when := s.network.Time()
	// Start participants.
	for _, p := range s.participants {
		if err := p.StartInstanceAt(instance, when); err != nil {
			panic(fmt.Errorf("participant %d failed starting: %w", p.ID(), err))
		}
	}
	// Start adversary
	if s.adversary != nil {
		if err := s.adversary.StartInstanceAt(instance, when); err != nil {
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
	var nextID gpbft.ActorID
	for _, archetype := range s.honestParticipantArchetypes {
		for i := 0; i < archetype.count; i++ {
			host := newHost(nextID, s, archetype.ecChainGenerator, archetype.storagePowerGenerator)
			pOpts := append(s.gpbftOptions, gpbft.WithTracer(host))
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
		_, currentRound, _ := participant.Progress()
		if currentRound > maxRound {
			maxRound = currentRound
		}
	}
	return maxRound
}
