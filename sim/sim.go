package sim

import (
	"fmt"
	"github.com/filecoin-project/go-f3/f3"
	"strings"
)

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount int
	LatencySeed int64
	LatencyMean float64
}

type Simulation struct {
	Network      *Network
	Base         f3.ECChain
	PowerTable   f3.PowerTable
	Beacon       []byte
	Participants []*f3.Participant
	Adversary    AdversaryReceiver
	CIDGen       *CIDGen
}

type AdversaryFactory func(id string, ntwk f3.Network) f3.Receiver

func NewSimulation(simConfig Config, graniteConfig f3.GraniteConfig, traceLevel int) *Simulation {
	// Create a network to deliver messages.
	lat := NewLogNormal(simConfig.LatencySeed, simConfig.LatencyMean)
	ntwk := NewNetwork(lat, traceLevel)
	vrf := f3.NewVRF(ntwk)

	// Create participants.
	genesisPower := f3.NewPowerTable(make([]f3.PowerEntry, 0))
	participants := make([]*f3.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		participants[i] = f3.NewParticipant(f3.ActorID(i), graniteConfig, ntwk, vrf)
		ntwk.AddParticipant(participants[i])
		genesisPower.Add(participants[i].ID(), f3.NewStoragePower(1), make([]byte, 0))
	}

	// Create genesis tipset, which all participants are expected to agree on as a base.
	genesis := f3.NewTipSet(100, f3.NewTipSetIDFromString("genesis"), 1)
	baseChain := f3.NewChain(genesis)
	return &Simulation{
		Network:      ntwk,
		Base:         baseChain,
		PowerTable:   *genesisPower,
		Beacon:       []byte("beacon"),
		Participants: participants,
		Adversary:    nil,
		CIDGen:       NewCIDGen(0x264803e715714f95), // Seed from Drand
	}
}

func (s *Simulation) SetAdversary(adv AdversaryReceiver, power uint) {
	s.Adversary = adv
	s.Network.AddParticipant(adv)
	s.PowerTable.Add(adv.ID(), f3.NewStoragePower(int64(power)), make([]byte, 0))
}

type ChainCount struct {
	Count int
	Chain f3.ECChain
}

// Delivers canonical chains to honest participants.
func (s *Simulation) ReceiveChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.Participants[pidx].ReceiveCanonicalChain(chain.Chain, s.PowerTable, s.Beacon)
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Sprintf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Delivers EC chains to honest participants.
func (s *Simulation) ReceiveECChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.Participants[pidx].ReceiveECChain(chain.Chain)
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Sprintf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(maxRounds uint32) bool {
	// Run until there are no more messages, meaning termination or deadlock.
	for s.Network.Tick(s.Adversary) && s.Participants[0].CurrentRound() <= maxRounds {
	}
	if s.Participants[0].CurrentRound() >= maxRounds {
		return false
	}
	first, _ := s.Participants[0].Finalised()
	for _, p := range s.Participants {
		f, _ := p.Finalised()
		if f.Eq(&f3.TipSet{}) {
			return false
		}
		if !f.Eq(&first) {
			return false
		}
	}
	return true
}

func (s *Simulation) PrintResults() {
	var firstFin f3.TipSet
	for _, p := range s.Participants {
		thisFin, _ := p.Finalised()
		if firstFin.Eq(&f3.TipSet{}) {
			firstFin = thisFin
		}
		if thisFin.Eq(&f3.TipSet{}) {
			fmt.Printf("‼️ Participant %d did not decide\n", p.ID())
		} else if !thisFin.Eq(&firstFin) {
			fmt.Printf("‼️ Participant %d decided %v, but %d decided %v\n", p.ID(), thisFin, s.Participants[0].ID(), firstFin)
		}
	}
}

func (s *Simulation) Describe() string {
	b := strings.Builder{}
	for _, p := range s.Participants {
		b.WriteString(p.Describe())
		b.WriteString("\n")
	}
	return b.String()
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

func (c *CIDGen) Sample() f3.TipSetID {
	b := make([]byte, 8)
	for i := range b {
		b[i] = alphanum[c.nextN(len(alphanum))]
	}
	return f3.NewTipSetID(b)
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

var alphanum = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
