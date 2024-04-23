package sim

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-f3/gpbft"
)

// One participant's host
// This provides methods that know the caller's participant ID and can provide its view of the world.
type SimHost struct {
	*Config
	*Network
	*EC
	*DecisionLog
	*TipGen
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
		newTip := v.TipGen.Sample()
		nextChain, _ := gpbft.NewChain(nextBase, newTip)

		v.EC.AddInstance(nextChain, nextPowerTable, nextBeacon)
		v.DecisionLog.BeginInstance(decision.Vote.Instance+1, nextBase, nextPowerTable)
	}
	elapsedEpochs := 1 //decision.Vote.Value.Head().Epoch - v.EC.BaseEpoch
	finalTimestamp := v.EC.BaseTimestamp.Add(time.Duration(elapsedEpochs) * v.Config.ECEpochDuration)
	// Next instance starts some fixed time after the next EC epoch is due.
	nextInstanceStart := finalTimestamp.Add(v.Config.ECEpochDuration).Add(v.Config.ECStabilisationDelay)
	return nextInstanceStart
}
