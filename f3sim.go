package main

import (
	"fmt"
	"github.com/anorth/f3sim/granite"
	"github.com/anorth/f3sim/net"
)

// TODO: CLI args etc
const ParticipantCount = 2

func main() {
	// Create a network to deliver messages.
	ntwk := net.New()

	// Create genesis chain, which all participants are expected to agree on as a base.
	genesis := net.ECChain{
		Base:      "genesis",
		BaseEpoch: 100,
		Tail:      []net.TipSet{},
	}

	// Create participants and add to network.
	participants := make([]*granite.Participant, ParticipantCount)
	for i := 0; i < ParticipantCount; i++ {
		participants[i] = granite.NewParticipant(fmt.Sprintf("participant-%d", i), ntwk)
		ntwk.AddParticipant(participants[i])
	}

	// TODO: Create a fake EC which will deliver tipsets to nodes
	for _, participant := range participants {
		participant.ReceiveCanonicalChain(genesis)
	}

	// Run until deadlock or some termination condition.
	for ntwk.Tick() {
	}
	fmt.Printf("No more messages\n")
}
