package sim

import (
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
	"math/rand"
)

type Config struct {
	ParticipantCount int
	AdversaryCount   int
	LatencySeed      int64
	LatencyMean      float64
	GraniteDelta     float64
}

type Simulation struct {
	Network      *net.Network
	Participants []*granite.Participant
	Adversaries  []net.Receiver
}

type AdversaryFactory func(id string, ntwk net.NetworkSink) net.Receiver

func NewSimulation(config *Config, adversary AdversaryFactory, traceLevel int) *Simulation {
	// Create a network to deliver messages.
	lat := net.NewLogNormal(config.LatencySeed, config.LatencyMean)
	ntwk := net.New(lat, traceLevel)

	// Create participants.
	participants := make([]*granite.Participant, config.ParticipantCount-config.AdversaryCount)
	for i := 0; i < len(participants); i++ {
		participants[i] = granite.NewParticipant(fmt.Sprintf("P%d", i), ntwk, config.GraniteDelta)
		ntwk.AddParticipant(participants[i])
	}
	adversaries := make([]net.Receiver, config.AdversaryCount)
	for i := 0; i < len(adversaries); i++ {
		adversaries[i] = adversary(fmt.Sprintf("A%d", i), ntwk)
		ntwk.AddParticipant(adversaries[i])
	}

	// Create genesis tipset, which all participants are expected to agree on as a base.
	genesisPower := net.NewPowerTable()
	for _, participant := range participants {
		genesisPower.Add(participant.ID(), 1)
	}
	for _, adversary := range adversaries {
		genesisPower.Add(adversary.ID(), 1)
	}
	genesis := net.NewTipSet(100, "genesis", 1, genesisPower)
	cidGen := NewCIDGen(0)

	// Create a candidate tipset for decision.
	candidate := net.ECChain{
		Base: genesis,
		Suffix: []net.TipSet{{
			Epoch:      genesis.Epoch + 1,
			CID:        cidGen.Sample(),
			Weight:     genesis.Weight + 1,
			PowerTable: genesis.PowerTable,
		}},
	}

	for _, participant := range participants {
		participant.ReceiveCanonicalChain(candidate)
	}
	for _, adversary := range adversaries {
		adversary.ReceiveCanonicalChain(candidate)
	}

	return &Simulation{
		Network:      ntwk,
		Participants: participants,
		Adversaries:  adversaries,
	}
}

func (s *Simulation) Run() bool {
	// Run until there are no more messages, meaning termination or deadlock.
	for s.Network.Tick() {
	}
	first := s.Participants[0].Finalised()
	for _, p := range s.Participants {
		f := p.Finalised()
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
		thisFin := p.Finalised()
		if firstFin.Eq(&net.TipSet{}) {
			firstFin = thisFin
		}
		if thisFin.Eq(&net.TipSet{}) {
			fmt.Printf("‼️ Participant %s did not decide\n", p.ID())
		} else if !thisFin.Eq(&firstFin) {
			fmt.Printf("‼️ Participant %s finalised %v, but %s finalised %v\n", p.ID(), thisFin, s.Participants[0].ID(), firstFin)
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
