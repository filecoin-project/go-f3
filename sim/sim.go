package sim

import (
	"fmt"
	"math"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
)

type Simulation struct {
	*options
	network      *Network
	ec           *EC
	participants []*gpbft.Participant
	adversary    *adversary.Adversary
	decisions    *DecisionLog
}

func NewSimulation(o ...Option) (*Simulation, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	network := newNetwork(opts)
	ec := newEC(opts)
	decisions := newDecisionLog(opts)

	s := &Simulation{
		options:   opts,
		network:   network,
		ec:        ec,
		decisions: decisions,
	}

	pOpts := append(opts.gpbftOptions, gpbft.WithTracer(s.network))
	var nextID gpbft.ActorID
	s.participants = make([]*gpbft.Participant, s.honestCount)
	for i := range s.participants {
		host := newHost(nextID, s)
		s.participants[i], err = gpbft.NewParticipant(nextID, host, pOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to instnatiate participant: %w", err)
		}
		pubKey, _ := s.signingBacked.GenerateKey()
		s.network.AddParticipant(s.participants[i])
		s.ec.AddParticipant(nextID, gpbft.NewStoragePower(1), pubKey)
		nextID++
	}

	// TODO: expand the simulation to accommodate more than one adversary for
	//       a more realistic simulation.
	// Limit adversaryCount to exactly one for now to reduce LOC up for review.
	// Future PRs will expand this to support a group of adversaries.
	if s.adversaryGenerator != nil && s.adversaryCount == 1 {
		host := newHost(nextID, s)
		s.adversary = s.adversaryGenerator(nextID, host)
		pubKey, _ := s.signingBacked.GenerateKey()
		s.network.AddParticipant(s.adversary)
		s.ec.AddParticipant(s.adversary.ID(), s.adversary.Power, pubKey)
	}

	s.decisions.BeginInstance(0, (*s.baseChain).Head(), s.ec.Instances[0].PowerTable)

	return s, nil
}

func (s *Simulation) PowerTable(instance uint64) *gpbft.PowerTable {
	return s.ec.Instances[instance].PowerTable
}

type ChainCount struct {
	Count int
	Chain gpbft.ECChain
}

// Sets canonical chains to be delivered to honest participants in the first instance.
func (s *Simulation) SetChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.ec.Instances[0].Chains[s.participants[pidx].ID()] = chain.Chain
			pidx += 1
		}
	}
	honestParticipantsCount := len(s.network.participantIDs)
	if pidx != s.HonestParticipantsCount() {
		panic(fmt.Errorf("%d participants but %d chains", honestParticipantsCount, pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(instanceCount uint64, maxRounds uint64) error {

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

	adversaryID := gpbft.ActorID(math.MaxUint64)
	if s.adversary != nil {
		adversaryID = s.adversary.ID()
	}
	finalInstance := instanceCount - 1
	currentInstance := uint64(0)

	// Run until there are no more messages, meaning termination or deadlock.
	moreTicks := true
	for moreTicks {
		var err error
		if s.decisions.err != nil {
			return fmt.Errorf("error in decision: %w", s.decisions.err)
		}
		if s.participants[0].CurrentRound() >= maxRounds {
			return fmt.Errorf("reached maximum number of %d rounds", maxRounds)
		}
		// Verify the current instance as soon as it completes.
		if s.decisions.HasCompletedInstance(currentInstance, adversaryID) {
			if err := s.decisions.VerifyInstance(currentInstance, adversaryID); err != nil {
				return fmt.Errorf("invalid decisions for instance %d: %w",
					currentInstance, err)
			}
			// Stop after all nodes have decided for the last instance.
			if currentInstance == finalInstance {
				break
			}
			currentInstance += 1
		}
		moreTicks, err = s.network.Tick(s.adversary)
		if err != nil {
			return fmt.Errorf("error performing simulation step: %w", err)
		}
	}
	return nil
}

// Returns the decision for a participant in an instance.
func (s *Simulation) GetDecision(instance uint64, participant gpbft.ActorID) (gpbft.ECChain, bool) {
	v, ok := s.decisions.Decisions[instance][participant]
	if ok {
		return v.Vote.Value, ok
	}
	return gpbft.ECChain{}, ok
}

func (s *Simulation) PrintResults() {
	s.decisions.PrintInstance(0)
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

func (s *Simulation) HonestParticipantsCount() int {
	return len(s.participants)
}

func (s *Simulation) GetInstance(i int) *ECInstance {
	if i < 0 || len(s.ec.Instances) <= i {
		return nil
	}
	return s.ec.Instances[i]
}
