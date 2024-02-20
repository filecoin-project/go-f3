package sim

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
)

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount int
	LatencySeed int64
	LatencyMean time.Duration
	// If nil then FakeSigner is used unless overriden by F3_TEST_USE_BLS
	SigningBacked *SigningBacked
}

func (c Config) UseBLS() Config {
	c.SigningBacked = BLSSigningBacked()
	return c
}

type SigningBacked struct {
	gpbft.Signer
	gpbft.Verifier
}

func FakeSigningBacked() *SigningBacked {
	fakeSigner := &FakeSigner{}
	return &SigningBacked{
		Signer:   fakeSigner,
		Verifier: fakeSigner,
	}
}

func BLSSigningBacked() *SigningBacked {
	return &SigningBacked{
		Signer:   blssig.SignerWithKeyOnG2(),
		Verifier: blssig.VerifierWithKeyOnG2(),
	}
}

type Simulation struct {
	Network      *Network
	Base         gpbft.ECChain
	PowerTable   gpbft.PowerTable
	Beacon       []byte
	Participants []*gpbft.Participant
	Adversary    AdversaryReceiver
	CIDGen       *CIDGen
}

type AdversaryFactory func(id string, ntwk gpbft.Network) gpbft.Receiver

func NewSimulation(simConfig Config, graniteConfig gpbft.GraniteConfig, traceLevel int) *Simulation {
	// Create a network to deliver messages.
	lat := NewLogNormal(simConfig.LatencySeed, simConfig.LatencyMean)
	sb := simConfig.SigningBacked

	if sb == nil {
		if os.Getenv("F3_TEST_USE_BLS") != "1" {
			sb = FakeSigningBacked()
		} else {
			sb = BLSSigningBacked()
		}
	}

	ntwk := NewNetwork(lat, traceLevel, *sb, "sim")

	// Create participants.
	genesisPower := gpbft.NewPowerTable(make([]gpbft.PowerEntry, 0))
	participants := make([]*gpbft.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		pubKey := ntwk.GenerateKey()
		vrf := gpbft.NewVRF(pubKey, ntwk.Signer, ntwk.Verifier)
		participants[i] = gpbft.NewParticipant(gpbft.ActorID(i), graniteConfig, ntwk, vrf)
		ntwk.AddParticipant(participants[i], pubKey)
		if err := genesisPower.Add(participants[i].ID(), gpbft.NewStoragePower(1), pubKey); err != nil {
			panic(fmt.Errorf("failed adding participant to power table: %w", err))
		}
	}

	// Create genesis tipset, which all participants are expected to agree on as a base.
	genesis := gpbft.NewTipSet(100, gpbft.NewTipSetIDFromString("genesis"))
	baseChain, err := gpbft.NewChain(genesis)
	if err != nil {
		panic(fmt.Errorf("failed creating new chain: %w", err))
	}

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
	pubKey := s.Network.GenerateKey()
	s.Network.AddParticipant(adv, pubKey)
	if err := s.PowerTable.Add(adv.ID(), gpbft.NewStoragePower(int64(power)), pubKey); err != nil {
		panic(err)
	}
}

type ChainCount struct {
	Count int
	Chain gpbft.ECChain
}

// Delivers canonical chains to honest participants.
func (s *Simulation) ReceiveChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			if err := s.Participants[pidx].ReceiveCanonicalChain(chain.Chain, s.PowerTable, s.Beacon); err != nil {
				panic(fmt.Errorf("participant %d failed receiving canonical chain %d: %w", pidx, i, err))
			}
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Errorf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Delivers EC chains to honest participants.
func (s *Simulation) ReceiveECChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			if err := s.Participants[pidx].ReceiveECChain(chain.Chain); err != nil {
				panic(err)
			}
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Errorf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(maxRounds uint64) error {
	var err error
	var moreTicks bool
	// Run until there are no more messages, meaning termination or deadlock.
	for moreTicks, err = s.Network.Tick(s.Adversary); err == nil && moreTicks && s.Participants[0].CurrentRound() <= maxRounds; moreTicks, err = s.Network.Tick(s.Adversary) {
	}
	if err != nil {
		return fmt.Errorf("error performing simulation step: %w", err)
	}
	if s.Participants[0].CurrentRound() >= maxRounds {
		return fmt.Errorf("reached maximum number of %d rounds", maxRounds)
	}
	first, _ := s.Participants[0].Finalised()
	for i, p := range s.Participants {
		f, _ := p.Finalised()
		if f.IsZero() {
			return fmt.Errorf("participant %d finalized empty tipset", i)
		}
		if f != first {
			return fmt.Errorf("finalized tipset mismatch between first participant and participant %d", i)
		}
	}
	return nil
}

func (s *Simulation) PrintResults() {
	var firstFin gpbft.TipSet
	for _, p := range s.Participants {
		thisFin, _ := p.Finalised()
		if firstFin.IsZero() {
			firstFin = thisFin
		}
		if thisFin.IsZero() {
			fmt.Printf("‼️ Participant %d did not decide\n", p.ID())
		} else if thisFin != firstFin {
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

func (c *CIDGen) Sample() gpbft.TipSetID {
	b := make([]byte, 8)
	for i := range b {
		b[i] = alphanum[c.nextN(len(alphanum))]
	}
	return gpbft.NewTipSetID(b)
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
