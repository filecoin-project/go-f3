package main

import (
	"flag"
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
	"math/rand"
	"time"
)

func main() {
	participantCount := flag.Int("participants", 2, "number of participants")
	iterations := flag.Int("iterations", 1, "number of simulation iterations")
	latencySeed := flag.Int64("latency-seed", int64(time.Now().UnixMilli()), "random seed for network latency")
	latencyMean := flag.Float64("latency-mean", 0.100, "mean network latency")
	graniteDelta := flag.Float64("granite-delta", 0.200, "granite delta parameter")
	traceLevel := flag.Int("trace", net.TraceNone, "trace verbosity level")
	flag.Parse()

	for i := 0; i < *iterations; i++ {
		// Increment seed for successive iterations.
		seed := *latencySeed + int64(i)
		fmt.Printf("Iteration %d: seed=%d, mean=%f\n", i, seed, *latencyMean)
		// Create a network to deliver messages.
		lat := net.NewLogNormal(seed, *latencyMean)
		ntwk := net.New(lat, *traceLevel)

		// Create participants.
		participants := make([]*granite.Participant, *participantCount)
		for i := 0; i < len(participants); i++ {
			participants[i] = granite.NewParticipant(fmt.Sprintf("P%d", i), ntwk, *graniteDelta)
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
		firstFin := participants[0].Finalised()
		for _, p := range participants {
			thisFin := p.Finalised()
			if firstFin.Eq(&net.TipSet{}) {
				firstFin = thisFin
			}
			if thisFin.Eq(&net.TipSet{}) {
				fmt.Printf("‼️ Participant %s did not decide\n", p.ID())
			} else if !thisFin.Eq(&firstFin) {
				fmt.Printf("‼️ Participant %s finalised %v, but %s finalised %v\n", p.ID(), thisFin, participants[0].ID(), firstFin)
			}
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
