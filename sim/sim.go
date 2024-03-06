package sim

import (
	"fmt"
	"golang.org/x/xerrors"
	"math"
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
	EC           *EC
	Participants []*gpbft.Participant
	Adversary    AdversaryReceiver
	Decisions    *DecisionLog
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
	baseChain, _ := gpbft.NewChain(gpbft.NewTipSet(100, gpbft.NewTipSetIDFromString("genesis")))
	beacon := []byte("beacon")
	ec := NewEC(baseChain, beacon)
	decisions := NewDecisionLog(ntwk, sb.Verifier)

	// Create participants.
	participants := make([]*gpbft.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		id := gpbft.ActorID(i)
		host := &SimHost{
			Network:     ntwk,
			EC:          ec,
			DecisionLog: decisions,
			id:          id,
		}
		participants[i] = gpbft.NewParticipant(id, graniteConfig, host)
		pubKey := ntwk.GenerateKey()
		ntwk.AddParticipant(participants[i], pubKey)
		ec.AddParticipant(id, gpbft.NewStoragePower(1), pubKey)
	}

	decisions.BeginInstance(0, baseChain.Head(), ec.PowerTable)
	return &Simulation{
		Network:      ntwk,
		EC:           ec,
		Participants: participants,
		Adversary:    nil,
		Decisions:    decisions,
		CIDGen:       NewCIDGen(0x264803e715714f95), // Seed from Drand
	}
}

func (s *Simulation) Base() gpbft.ECChain {
	return s.EC.Base
}

func (s *Simulation) PowerTable() *gpbft.PowerTable {
	return s.EC.PowerTable
}

func (s *Simulation) SetAdversary(adv AdversaryReceiver, power uint) {
	s.Adversary = adv
	pubKey := s.Network.GenerateKey()
	s.Network.AddParticipant(adv, pubKey)
	s.EC.AddParticipant(adv.ID(), gpbft.NewStoragePower(int64(power)), pubKey)
}

func (s *Simulation) HostFor(id gpbft.ActorID) *SimHost {
	return &SimHost{
		Network: s.Network,
		EC:      s.EC,
		id:      id,
	}
}

type ChainCount struct {
	Count int
	Chain gpbft.ECChain
}

// Sets canonical chains to be delivered to honest participants,
// then starts each participant.
func (s *Simulation) SetChains(chains ...ChainCount) {
	pidx := 0
	for _, chain := range chains {
		for i := 0; i < chain.Count; i++ {
			s.EC.Chains[s.Participants[pidx].ID()] = chain.Chain
			pidx += 1
		}
	}
	if pidx != len(s.Participants) {
		panic(fmt.Errorf("%d participants but %d chains", len(s.Participants), pidx))
	}
}

// Runs simulation, and returns whether all participants decided on the same value.
func (s *Simulation) Run(maxRounds uint64) error {
	// Start participants.
	for _, p := range s.Participants {
		if err := p.Start(); err != nil {
			panic(fmt.Errorf("participant %d failed starting: %w", p.ID(), err))
		}
	}

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
	adversaryID := gpbft.ActorID(math.MaxUint64)
	if s.Adversary != nil {
		adversaryID = s.Adversary.ID()
	}
	if err := s.Decisions.CompleteInstance(0, adversaryID); err != nil {
		return fmt.Errorf("incompatible or incomplete decisions: %w", err)
	}
	return nil
}

// Returns the decision for a participant in an instance.
func (s *Simulation) GetDecision(instance uint64, participant gpbft.ActorID) (gpbft.ECChain, bool) {
	v, ok := s.Decisions.Decisions[instance][participant]
	return v.Vote.Value, ok
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

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type SimHost struct {
	*Network
	*EC
	*DecisionLog
	id gpbft.ActorID
}

var _ gpbft.Host = (*SimHost)(nil)

///// Chain interface

func (v *SimHost) GetCanonicalChain() (chain gpbft.ECChain, power gpbft.PowerTable, beacon []byte) {
	chain = v.Chains[v.id]
	power = *v.PowerTable
	beacon = v.Beacon
	return
}

func (v *SimHost) SetAlarm(at time.Time) {
	v.Network.SetAlarm(v.id, at)
}

func (v *SimHost) ReceiveDecision(decision gpbft.Justification) {
	v.DecisionLog.ReceiveDecision(v.id, decision)
}

// Receives and validates finality decisions
type DecisionLog struct {
	Network  gpbft.Network
	Verifier gpbft.Verifier
	// Base tipset for each round.
	Bases map[uint64]gpbft.TipSet
	// Powertable for each round.
	PowerTables map[uint64]*gpbft.PowerTable
	// Decisions received for each round and participant.
	Decisions map[uint64]map[gpbft.ActorID]gpbft.Justification
	err       error
}

func NewDecisionLog(ntwk gpbft.Network, verifier gpbft.Verifier) *DecisionLog {
	return &DecisionLog{
		Network:     ntwk,
		Verifier:    verifier,
		Bases:       make(map[uint64]gpbft.TipSet),
		PowerTables: make(map[uint64]*gpbft.PowerTable),
		Decisions:   make(map[uint64]map[gpbft.ActorID]gpbft.Justification),
	}
}

// Establishes the base tipset and power table for a new instance.
func (dl *DecisionLog) BeginInstance(instance uint64, base gpbft.TipSet, power *gpbft.PowerTable) {
	dl.Bases[instance] = base
	dl.PowerTables[instance] = power
}

// Checks that all participants for an instance decided on the same value.
func (dl *DecisionLog) CompleteInstance(instance uint64, adversary gpbft.ActorID) error {
	if dl.err != nil {
		return dl.err
	}
	powerTable := dl.PowerTables[instance]
	decisions := dl.Decisions[instance]

	// Check each actor in the power table decided, and their decisions matched.
	var first gpbft.ECChain
	for _, powerEntry := range powerTable.Entries {
		decision, ok := decisions[powerEntry.ID]
		if powerEntry.ID == adversary {
			continue
		}
		if !ok {
			return fmt.Errorf("actor %d did not decide", powerEntry.ID)
		}
		if first.IsZero() {
			first = decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(first) {
			return fmt.Errorf("actor %d decided %v, but first actor decided %v",
				powerEntry.ID, decision.Vote.Value, first)
		}
	}
	return nil
}

func (dl *DecisionLog) PrintInstance(instance uint64) {
	powerTable := dl.PowerTables[instance]
	decisions := dl.Decisions[instance]

	var first gpbft.ECChain
	for _, powerEntry := range powerTable.Entries {
		decision, ok := decisions[powerEntry.ID]
		if !ok {
			fmt.Printf("‼️ Participant %d did not decide\n", powerEntry.ID)
		}
		if first.IsZero() {
			first = decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(first) {
			fmt.Printf("‼️ Participant %d decided %v, but %d decided %v\n",
				powerEntry.ID, decision.Vote, powerTable.Entries[0].ID, first)
		}
	}
}

// Verifies and records a decision from a participant.
func (dl *DecisionLog) ReceiveDecision(participant gpbft.ActorID, decision gpbft.Justification) {
	if err := dl.verifyDecision(&decision); err == nil {
		if dl.Decisions[decision.Vote.Instance] == nil {
			dl.Decisions[decision.Vote.Instance] = make(map[gpbft.ActorID]gpbft.Justification)
		}
		dl.Decisions[decision.Vote.Instance][participant] = decision
	} else {
		fmt.Printf("invalid decision: %v\n", err)
		dl.err = err
	}
}

func (dl *DecisionLog) verifyDecision(decision *gpbft.Justification) error {
	base := dl.Bases[decision.Vote.Instance]
	powerTable := dl.PowerTables[decision.Vote.Instance]
	if decision.Vote.Step != gpbft.DECIDE_PHASE {
		return fmt.Errorf("decision for wrong phase: %v", decision)
	}
	if decision.Vote.Round != 0 {
		return fmt.Errorf("decision for wrong round: %v", decision)
	}
	if decision.Vote.Value.IsZero() {
		return fmt.Errorf("finalized empty tipset: %v", decision)
	}
	if !decision.Vote.Value.HasBase(base) {
		return fmt.Errorf("finalized tipset with wrong base: %v", decision)
	}

	// Extract signers.
	justificationPower := gpbft.NewStoragePower(0)
	signers := make([]gpbft.PubKey, 0)
	if err := decision.Signers.ForEach(func(bit uint64) error {
		if int(bit) >= len(powerTable.Entries) {
			return fmt.Errorf("invalid signer index: %d", bit)
		}
		justificationPower.Add(justificationPower, powerTable.Entries[bit].Power)
		signers = append(signers, powerTable.Entries[bit].PubKey)
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over signers: %w", err)
	}
	// Check signers have strong quorum
	strongQuorum := gpbft.NewStoragePower(0)
	strongQuorum = strongQuorum.Mul(strongQuorum, gpbft.NewStoragePower(2))
	strongQuorum = strongQuorum.Div(strongQuorum, gpbft.NewStoragePower(3))
	if justificationPower.Cmp(strongQuorum) < 0 {
		return fmt.Errorf("decision lacks strong quorum: %v", decision)
	}
	// Verify aggregate signature
	payload := decision.Vote.MarshalForSigning(dl.Network.NetworkName())
	if err := dl.Verifier.VerifyAggregate(payload, decision.Signature, signers); err != nil {
		return xerrors.Errorf("invalid aggregate signature: %v: %w", decision, err)
	}
	return nil
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
