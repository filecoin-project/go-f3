package sim

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/filecoin-project/go-f3/sim/latency"
	"github.com/filecoin-project/go-f3/sim/signing"
)

type Simulation struct {
	Config       Config
	Network      *Network
	EC           *EC
	Participants []*gpbft.Participant
	Adversary    adversary.Receiver
	Decisions    *DecisionLog
	TipGen       *TipGen
}

func NewSimulation(simConfig Config, graniteConfig gpbft.GraniteConfig, traceLevel int) (*Simulation, error) {
	// Create a network to deliver messages.
	lat, err := latency.NewLogNormal(simConfig.LatencySeed, simConfig.LatencyMean)
	if err != nil {
		return nil, err
	}
	sb := simConfig.SigningBacked

	if sb == nil {
		if os.Getenv("F3_TEST_USE_BLS") != "1" {
			sb = signing.NewFakeBackend()
		} else {
			sb = signing.NewBLSBackend()
		}
	}

	ntwk := NewNetwork(lat, traceLevel, sb, "sim")
	baseChain, _ := gpbft.NewChain([]byte(("genesis")))
	beacon := []byte("beacon")
	ec := NewEC(baseChain, beacon)
	tipGen := NewTipGen(0x264803e715714f95) // Seed from Drand
	decisions := NewDecisionLog(ntwk, sb)

	// Create participants.
	participants := make([]*gpbft.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		id := gpbft.ActorID(i)
		host := &SimHost{
			Config:      &simConfig,
			Network:     ntwk,
			EC:          ec,
			DecisionLog: decisions,
			TipGen:      tipGen,
			id:          id,
		}
		participants[i] = gpbft.NewParticipant(id, graniteConfig, host, host, 0)
		pubKey, _ := sb.GenerateKey()
		ntwk.AddParticipant(participants[i], pubKey)
		ec.AddParticipant(id, gpbft.NewStoragePower(1), pubKey)
	}

	decisions.BeginInstance(0, baseChain.Head(), ec.Instances[0].PowerTable)
	return &Simulation{
		Config:       simConfig,
		Network:      ntwk,
		EC:           ec,
		Participants: participants,
		Adversary:    nil,
		Decisions:    decisions,
		TipGen:       tipGen,
	}, nil
}

func (s *Simulation) Base(instance uint64) gpbft.ECChain {
	return s.EC.Instances[instance].Base
}

func (s *Simulation) PowerTable(instance uint64) *gpbft.PowerTable {
	return s.EC.Instances[instance].PowerTable
}

func (s *Simulation) SetAdversary(adv adversary.Receiver, power uint) {
	s.Adversary = adv
	pubKey, _ := s.Network.GenerateKey()
	s.Network.AddParticipant(adv, pubKey)
	s.EC.AddParticipant(adv.ID(), gpbft.NewStoragePower(int64(power)), pubKey)
}

func (s *Simulation) HostFor(id gpbft.ActorID) *SimHost {
	return &SimHost{
		Config:      &s.Config,
		Network:     s.Network,
		EC:          s.EC,
		DecisionLog: s.Decisions,
		TipGen:      s.TipGen,
		id:          id,
	}
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
			s.EC.Instances[0].Chains[s.Participants[pidx].ID()] = chain.Chain
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Errorf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(instanceCount uint64, maxRounds uint64) error {
	// Start participants.
	for _, p := range s.Participants {
		if err := p.Start(); err != nil {
			panic(fmt.Errorf("participant %d failed starting: %w", p.ID(), err))
		}
	}

	adversaryID := gpbft.ActorID(math.MaxUint64)
	if s.Adversary != nil {
		adversaryID = s.Adversary.ID()
	}
	finalInstance := instanceCount - 1
	currentInstance := uint64(0)

	// Run until there are no more messages, meaning termination or deadlock.
	moreTicks := true
	for moreTicks {
		var err error
		if s.Decisions.err != nil {
			return fmt.Errorf("error in decision: %w", s.Decisions.err)
		}
		if s.Participants[0].CurrentRound() >= maxRounds {
			return fmt.Errorf("reached maximum number of %d rounds", maxRounds)
		}
		// Verify the current instance as soon as it completes.
		if s.Decisions.HasCompletedInstance(currentInstance, adversaryID) {
			if err := s.Decisions.VerifyInstance(currentInstance, adversaryID); err != nil {
				return fmt.Errorf("invalid decisions for instance %d: %w",
					currentInstance, err)
			}
			// Stop after all nodes have decided for the last instance.
			if currentInstance == finalInstance {
				break
			}
			currentInstance += 1
		}
		moreTicks, err = s.Network.Tick(s.Adversary)
		if err != nil {
			return fmt.Errorf("error performing simulation step: %w", err)
		}
	}
	return nil
}

// Returns the decision for a participant in an instance.
func (s *Simulation) GetDecision(instance uint64, participant gpbft.ActorID) (gpbft.ECChain, bool) {
	v, ok := s.Decisions.Decisions[instance][participant]
	if ok {
		return v.Vote.Value, ok
	}
	return gpbft.ECChain{}, ok
}

func (s *Simulation) PrintResults() {
	s.Decisions.PrintInstance(0)
}

func (s *Simulation) Describe() string {
	b := strings.Builder{}
	for _, p := range s.Participants {
		b.WriteString(p.Describe())
		b.WriteString("\n")
	}
	return b.String()
}
