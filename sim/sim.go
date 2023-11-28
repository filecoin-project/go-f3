package sim

import (
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
)

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount int
	LatencySeed int64
	LatencyMean float64
}

type Simulation struct {
	Network      *net.Network
	Participants []*granite.Participant
	Adversary    net.AdversaryReceiver
	Base         *net.ECChain
	CIDGen       *CIDGen
}

type AdversaryFactory func(id string, ntwk net.NetworkSink) net.Receiver

func NewSimulation(simConfig Config, graniteConfig granite.Config, traceLevel int) *Simulation {
	// Create a network to deliver messages.
	lat := net.NewLogNormal(simConfig.LatencySeed, simConfig.LatencyMean)
	ntwk := net.New(lat, traceLevel)
	vrf := granite.NewFakeVRF()

	// Create participants.
	genesisPower := net.NewPowerTable()
	participants := make([]*granite.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		participants[i] = granite.NewParticipant(fmt.Sprintf("P%d", i), graniteConfig, ntwk, vrf)
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
		CIDGen:       NewCIDGen(0x264803e715714f95), // Seed from Drand
	}
}

func (s *Simulation) SetAdversary(adv net.AdversaryReceiver, power uint) {
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
	beacon := []byte("beacon")
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.Participants[pidx].ReceiveCanonicalChain(chain.Chain, beacon)
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Sprintf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(maxRounds int) bool {
	// Run until there are no more messages, meaning termination or deadlock.
	for s.Network.Tick(s.Adversary) && s.Participants[0].CurrentRound() <= maxRounds {
	}
	if s.Participants[0].CurrentRound() >= maxRounds {
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

// A CID generator.
// This uses a fast xorshift PRNG to generate random CIDs.
// The statistical properties of these CIDs are not important to correctness.
type CIDGen struct {
	xorshiftState uint64
}

func NewCIDGen(seed uint64) *CIDGen {
	return &CIDGen{seed}
}

func (c *CIDGen) Sample() net.CID {
	b := make([]rune, 8)
	for i := range b {
		b[i] = alphanum[c.nextN(len(alphanum))]
	}
	return string(b)
}

func (c *CIDGen) nextN(n int) uint64 {
	bucketSize := uint64(1<<63) / uint64(n)
	limit := bucketSize * uint64(n)
	for {
		x := c.next()
		if x < limit {
			return x / bucketSize
		}
	}
}

func (c *CIDGen) next() uint64 {
	x := c.xorshiftState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	c.xorshiftState = x
	return x
}

var alphanum = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
