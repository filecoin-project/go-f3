package main

import (
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
	"math/rand"
	"time"
)

// TODO: CLI args etc
const ParticipantCount = 2

func main() {
	// Create a network to deliver messages.
	latencySeed := int64(time.Now().Nanosecond())
	latencyMean := 0.100
	lat := net.NewLogNormal(latencySeed, latencyMean)
	ntwk := net.New(lat)
	delta := latencyMean * 2

	// Create participants.
	participants := make([]*granite.Participant, ParticipantCount)
	for i := 0; i < ParticipantCount; i++ {
		participants[i] = granite.NewParticipant(fmt.Sprintf("P%d", i), ntwk, delta)
		ntwk.AddParticipant(participants[i])
	}

	// Create genesis chain, which all participants are expected to agree on as a base.
	genesisPower := net.NewPowerTable()
	for _, participant := range participants {
		genesisPower.Add(participant.ID(), 1)
	}
	genesis := net.NewTipSet(100, "genesis", 1, genesisPower)
	cidGen := NewCIDGen(0)
	candidate := net.ECChain{
		Base: genesis,
		Suffix: []net.TipSet{{
			Epoch:      genesis.Epoch + 1,
			CID:        cidGen.Sample(),
			Weight:     genesis.Weight + 1,
			PowerTable: genesis.PowerTable,
		}},
	}

	// TODO: Create a fake EC which will deliver subsequent tipsets to nodes
	for _, participant := range participants {
		participant.ReceiveCanonicalChain(candidate)
	}

	// Run until there are no more messages, meaning termination or deadlock.
	for ntwk.Tick() {
	}
	decision := participants[0].Finalised()
	for _, p := range participants {
		d := p.Finalised()
		if d.Eq(&net.TipSet{}) {
			fmt.Printf("‼️ Participant %s did not finalise\n", p.ID())
		}
		if decision.Eq(&net.TipSet{}) {
			decision = d
		}
		if !decision.Eq(&d) {
			fmt.Printf("‼️ Participant %s finalised %v, but %s finalised %v\n", participants[0].ID(), decision, p.ID(), d)
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
