package sim

import (
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
	"math/rand"
)

const MAX_ROUNDS = 10

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount  int
	LatencySeed  int64
	LatencyMean  float64
	GraniteDelta float64
}

type Simulation struct {
	Network      *net.Network
	Participants []*granite.Participant
	Adversary    net.AdversaryInterceptor
	Base         *net.ECChain
	CIDGen       *CIDGen
}

type AdversaryFactory func(id string, ntwk net.NetworkSink) net.Receiver

func NewSimulation(config *Config, traceLevel int) *Simulation {
	// Create a network to deliver messages.
	lat := net.NewLogNormal(config.LatencySeed, config.LatencyMean)
	ntwk := net.New(lat, traceLevel)

	// Create participants.
	genesisPower := net.NewPowerTable()
	participants := make([]*granite.Participant, config.HonestCount)
	for i := 0; i < len(participants); i++ {
		participants[i] = granite.NewParticipant(fmt.Sprintf("P%d", i), ntwk, config.GraniteDelta)
		ntwk.AddParticipant(participants[i])
		genesisPower.Add(participants[i].ID(), 1)
	}

	// Create genesis tipset, which all participants are expected to agree on as a base.
	genesis := net.NewTipSet(100, "genesis", 1, genesisPower)
	baseChain := net.NewChain(genesis)
	return &Simulation{
		Network:      ntwk,
		Base:         baseChain,
		Participants: participants,
		Adversary:    nil,
		CIDGen:       NewCIDGen(0),
	}
}

func (s *Simulation) SetAdversary(adv net.AdversaryInterceptor, power uint) {
	s.Adversary = adv
	s.Network.AddParticipant(adv)
	s.Base.Head().PowerTable.Add(adv.ID(), power)
}

type ChainCount struct {
	Count int
	Chain net.ECChain
}

// Delivers canonical chains to honest participants.
func (s *Simulation) ReceiveChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.Participants[pidx].ReceiveCanonicalChain(chain.Chain)
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Sprintf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run() bool {
	// Run until there are no more messages, meaning termination or deadlock.
	for s.Network.Tick(s.Adversary) && s.Participants[0].CurrentRound() <= MAX_ROUNDS {
	}
	if s.Participants[0].CurrentRound() >= MAX_ROUNDS {
		return false
	}
	first, _ := s.Participants[0].Finalised()
	for _, p := range s.Participants {
		f, _ := p.Finalised()
		if f.Eq(&net.TipSet{}) {
			return false
		}
		if !f.Eq(&first) {
			return false
		}
	}
	return true
}

func (s *Simulation) PrintResults() {
	if s.Participants[0].CurrentRound() >= MAX_ROUNDS {
		fmt.Println("‼️ Max rounds reached without decision")
		return
	}
	var firstFin net.TipSet
	for _, p := range s.Participants {
		thisFin, _ := p.Finalised()
		if firstFin.Eq(&net.TipSet{}) {
			firstFin = thisFin
		}
		if thisFin.Eq(&net.TipSet{}) {
			fmt.Printf("‼️ Participant %s did not decide\n", p.ID())
		} else if !thisFin.Eq(&firstFin) {
			fmt.Printf("‼️ Participant %s decided %v, but %s decided %v\n", p.ID(), thisFin, s.Participants[0].ID(), firstFin)
		}
	}
}

type CIDGen struct {
	rng *rand.Rand
}

func NewCIDGen(seed int64) *CIDGen {
	return &CIDGen{rng: rand.New(rand.NewSource(seed))}
}

func (c *CIDGen) Sample() net.CID {
	b := make([]rune, 8)
	for i := range b {
		b[i] = alphanum[rand.Intn(len(alphanum))]
	}
	return string(b)
}

var alphanum = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
