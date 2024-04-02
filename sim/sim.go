package sim

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-f3/blssig"
	"github.com/filecoin-project/go-f3/gpbft"
)

type Config struct {
	// Honest participant count.
	// Honest participants have one unit of power each.
	HonestCount int
	LatencySeed int64
	// Mean delivery latency for messages.
	LatencyMean time.Duration
	// Duration of EC epochs.
	ECEpochDuration time.Duration
	// Time to wait after EC epoch before starting next instance.
	ECStabilisationDelay time.Duration
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
	Config       Config
	Network      *Network
	EC           *EC
	Participants []*gpbft.Participant
	Adversary    AdversaryReceiver
	Decisions    *DecisionLog
	CIDGen       *TipIDGen
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
	cidGen := NewCIDGen(0x264803e715714f95) // Seed from Drand
	decisions := NewDecisionLog(ntwk, sb.Verifier)

	// Create participants.
	participants := make([]*gpbft.Participant, simConfig.HonestCount)
	for i := 0; i < len(participants); i++ {
		id := gpbft.ActorID(i)
		host := &SimHost{
			Config:      &simConfig,
			Network:     ntwk,
			EC:          ec,
			DecisionLog: decisions,
			TipIDGen:    cidGen,
			id:          id,
		}
		participants[i] = gpbft.NewParticipant(id, graniteConfig, host, host)
		pubKey := ntwk.GenerateKey()
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
		CIDGen:       cidGen,
	}
}

func (s *Simulation) Base(instance uint64) gpbft.ECChain {
	return s.EC.Instances[instance].Base
}

func (s *Simulation) PowerTable(instance uint64) *gpbft.PowerTable {
	return s.EC.Instances[instance].PowerTable
}

func (s *Simulation) SetAdversary(adv AdversaryReceiver, power uint) {
	s.Adversary = adv
	pubKey := s.Network.GenerateKey()
	s.Network.AddParticipant(adv, pubKey)
	s.EC.AddParticipant(adv.ID(), gpbft.NewStoragePower(int64(power)), pubKey)
}

func (s *Simulation) HostFor(id gpbft.ActorID) *SimHost {
	return &SimHost{
		Config:      &s.Config,
		Network:     s.Network,
		EC:          s.EC,
		DecisionLog: s.Decisions,
		TipIDGen:    s.CIDGen,
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

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type SimHost struct {
	*Config
	*Network
	*EC
	*DecisionLog
	*TipIDGen
	id gpbft.ActorID
}

var _ gpbft.Host = (*SimHost)(nil)

///// Chain interface

func (v *SimHost) GetCanonicalChain() (chain gpbft.ECChain, power gpbft.PowerTable, beacon []byte) {
	// Find the instance after the last instance finalised by the participant.
	instance := uint64(0)
	for i := len(v.Decisions) - 1; i >= 0; i-- {
		if v.Decisions[i][v.id] != nil {
			instance = uint64(i + 1)
			break
		}
	}

	i := v.Instances[instance]
	chain = i.Chains[v.id]
	power = *i.PowerTable
	beacon = i.Beacon
	return
}

func (v *SimHost) SetAlarm(at time.Time) {
	v.Network.SetAlarm(v.id, at)
}

func (v *SimHost) ReceiveDecision(decision *gpbft.Justification) time.Time {
	firstForInstance := v.DecisionLog.ReceiveDecision(v.id, decision)
	if firstForInstance {
		// When the first valid decision is received for an instance, prepare for the next one.
		nextBase := decision.Vote.Value.Head()
		// Copy the previous instance power table.
		// The simulator doesn't have any facility to evolve the power table.
		// See https://github.com/filecoin-project/go-f3/issues/114.
		nextPowerTable := v.EC.Instances[decision.Vote.Instance].PowerTable.Copy()
		nextBeacon := []byte(fmt.Sprintf("beacon %d", decision.Vote.Instance+1))
		// Create a new chain for all participants.
		// There's no facility yet for them to observe different chains after the first instance.
		// See https://github.com/filecoin-project/go-f3/issues/115.
		newTip := gpbft.NewTipSet(nextBase.Epoch+1, v.TipIDGen.Sample())
		nextChain, _ := gpbft.NewChain(nextBase, newTip)

		v.EC.AddInstance(nextChain, nextPowerTable, nextBeacon)
		v.DecisionLog.BeginInstance(decision.Vote.Instance+1, nextBase, nextPowerTable)
	}
	elapsedEpochs := decision.Vote.Value.Head().Epoch - v.EC.BaseEpoch
	finalTimestamp := v.EC.BaseTimestamp.Add(time.Duration(elapsedEpochs) * v.Config.ECEpochDuration)
	// Next instance starts some fixed time after the next EC epoch is due.
	nextInstanceStart := finalTimestamp.Add(v.Config.ECEpochDuration).Add(v.Config.ECStabilisationDelay)
	return nextInstanceStart
}

// Receives and validates finality decisions
type DecisionLog struct {
	Network  gpbft.Network
	Verifier gpbft.Verifier
	// Base tipset for each instance.
	Bases []gpbft.TipSet
	// Powertable for each instance.
	PowerTables []*gpbft.PowerTable
	// Decisions received for each instance and participant.
	Decisions []map[gpbft.ActorID]*gpbft.Justification
	err       error
}

func NewDecisionLog(ntwk gpbft.Network, verifier gpbft.Verifier) *DecisionLog {
	return &DecisionLog{
		Network:     ntwk,
		Verifier:    verifier,
		Bases:       nil,
		PowerTables: nil,
		Decisions:   nil,
	}
}

// Establishes the base tipset and power table for a new instance.
func (dl *DecisionLog) BeginInstance(instance uint64, base gpbft.TipSet, power *gpbft.PowerTable) {
	if instance != uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d is not the next instance", instance))
	}
	dl.Bases = append(dl.Bases, base)
	dl.PowerTables = append(dl.PowerTables, power)
	dl.Decisions = append(dl.Decisions, make(map[gpbft.ActorID]*gpbft.Justification))
}

// Checks whether all participants (except any adversary) have decided on a value for an instance.
func (dl *DecisionLog) HasCompletedInstance(instance uint64, adversary gpbft.ActorID) bool {
	if instance >= uint64(len(dl.Bases)) {
		return false
	}
	target := len(dl.PowerTables[instance].Entries)
	if dl.PowerTables[instance].Has(adversary) {
		target -= 1
	}

	return len(dl.Decisions[instance]) == target
}

// Checks that all participants (except any adversary) for an instance decided on the same value.
func (dl *DecisionLog) VerifyInstance(instance uint64, adversary gpbft.ActorID) error {
	if dl.err != nil {
		return dl.err
	}
	if instance >= uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d not yet begun", instance))
	}
	// Check each actor in the power table decided, and their decisions matched.
	var first gpbft.ECChain
	for _, powerEntry := range dl.PowerTables[instance].Entries {
		decision, ok := dl.Decisions[instance][powerEntry.ID]
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
	if instance >= uint64(len(dl.Bases)) {
		panic(fmt.Errorf("instance %d not yet begun", instance))
	}
	var first gpbft.ECChain
	for _, powerEntry := range dl.PowerTables[instance].Entries {
		decision, ok := dl.Decisions[instance][powerEntry.ID]
		if !ok {
			fmt.Printf("‼️ Participant %d did not decide\n", powerEntry.ID)
		}
		if first.IsZero() {
			first = decision.Vote.Value
		}
		if !decision.Vote.Value.Eq(first) {
			fmt.Printf("‼️ Participant %d decided %v, but %d decided %v\n",
				powerEntry.ID, decision.Vote, dl.PowerTables[instance].Entries[0].ID, first)
		}
	}
}

// Verifies and records a decision from a participant.
// Returns whether this was the first decision for the instance.
func (dl *DecisionLog) ReceiveDecision(participant gpbft.ActorID, decision *gpbft.Justification) (first bool) {
	if err := dl.verifyDecision(decision); err == nil {
		if len(dl.Decisions[decision.Vote.Instance]) == 0 {
			first = true
		}
		// Record the participant's decision.
		dl.Decisions[decision.Vote.Instance][participant] = decision
	} else {
		fmt.Printf("invalid decision: %v\n", err)
		dl.err = err
	}
	return
}

func (dl *DecisionLog) verifyDecision(decision *gpbft.Justification) error {
	if decision.Vote.Instance >= uint64(len(dl.Bases)) {
		return fmt.Errorf("decision for future instance: %v", decision)
	}
	base := dl.Bases[decision.Vote.Instance]
	powerTable := dl.PowerTables[decision.Vote.Instance]
	if decision.Vote.Step != gpbft.DECIDE_PHASE {
		return fmt.Errorf("decision for wrong phase: %v", decision)
	}
	if decision.Vote.Round != 0 {
		return fmt.Errorf("decision for wrong round: %v", decision)
	}
	if decision.Vote.Value.IsZero() {
		return fmt.Errorf("decided empty tipset: %v", decision)
	}
	if !decision.Vote.Value.HasBase(base) {
		return fmt.Errorf("decided tipset with wrong base: %v", decision)
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

// A tipset ID generator.
// This uses a fast xorshift PRNG to generate random tipset IDs.
// The statistical properties of these are not important to correctness.
type TipIDGen struct {
	xorshiftState uint64
}

func NewCIDGen(seed uint64) *TipIDGen {
	return &TipIDGen{seed}
}

func (c *TipIDGen) Sample() gpbft.TipSetID {
	b := make([]byte, 8)
	for i := range b {
		b[i] = alphanum[c.nextN(len(alphanum))]
	}
	return gpbft.NewTipSetID([][]byte{b})
}

func (c *TipIDGen) nextN(n int) uint64 {
	bucketSize := uint64(1<<63) / uint64(n)
	limit := bucketSize * uint64(n)
	for {
		x := c.next()
		if x < limit {
			return x / bucketSize
		}
	}
}

func (c *TipIDGen) next() uint64 {
	x := c.xorshiftState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	c.xorshiftState = x
	return x
}

var alphanum = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
