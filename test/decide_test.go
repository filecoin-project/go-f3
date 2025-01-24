package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-f3/gpbft"
	"github.com/filecoin-project/go-f3/sim"
	"github.com/filecoin-project/go-f3/sim/adversary"
	"github.com/stretchr/testify/require"
)

func FuzzImmediateDecideAdversary(f *testing.F) {
	f.Add(98562314)
	f.Add(8)
	f.Add(-9554)
	f.Add(95)
	f.Add(65)
	f.Fuzz(func(t *testing.T, seed int) {
		t.Parallel()
		rng := rand.New(rand.NewSource(int64(seed)))
		tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
		baseChain := generateECChain(t, tsg)
		adversaryValue := baseChain.Extend(tsg.Sample())
		sm, err := sim.NewSimulation(
			asyncOptions(rng.Int(),
				sim.AddHonestParticipants(
					1,
					sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
					uniformOneStoragePower),
				sim.WithBaseChain(baseChain),
				// Add the adversary to the simulation with 3/4 of total power.
				sim.WithAdversary(adversary.NewImmediateDecideGenerator(adversaryValue, gpbft.NewStoragePower(3))),
			)...)
		require.NoError(t, err)

		err = sm.Run(1, maxRounds)
		if err != nil {
			fmt.Printf("%s", sm.Describe())
			sm.GetInstance(0).Print()
		}
		require.NoError(t, err)

		decision := sm.GetInstance(0).GetDecision(0)
		require.NotNil(t, decision, "no decision")
		require.Equal(t, adversaryValue.Head(), decision.Head(), "honest node did not decide the right value")
	})
}

func TestIllegalCommittee_OutOfRange(t *testing.T) {
	const seed = 98562314
	t.Parallel()
	rng := rand.New(rand.NewSource(int64(seed)))
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(
		asyncOptions(rng.Int(),
			sim.AddHonestParticipants(
				1,
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
				uniformOneStoragePower),
			sim.WithBaseChain(baseChain),
			// Add the adversary to the simulation with 3/4 of total power.
			sim.WithAdversary(adversary.NewImmediateDecideGenerator(
				adversaryValue,
				gpbft.NewStoragePower(3),
				adversary.ImmediateDecideWithNthParticipant(100))),
		)...)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	require.ErrorContains(t, err, "invalid signer index")
}

func TestIllegalCommittee_NoPower(t *testing.T) {
	const seed = 98562314
	t.Parallel()
	rng := rand.New(rand.NewSource(int64(seed)))
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(
		asyncOptions(rng.Int(),
			sim.AddHonestParticipants(
				1,
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
				sim.UniformStoragePower(gpbft.NewStoragePower(1))),
			sim.WithBaseChain(baseChain),
			// Add the adversary to the simulation with enough power to make the honest
			// node's power a rounding error. Then have that adversary include the
			// honest participant in their decision (which is illegal because the
			// decision has no effective power due to rounding).
			sim.WithAdversary(adversary.NewImmediateDecideGenerator(
				adversaryValue,
				gpbft.NewStoragePower(0xffff),
				adversary.ImmediateDecideWithNthParticipant(1))),
		)...)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	require.ErrorContains(t, err, "signer with ID 0 has no power")
}

func TestIllegalCommittee_WrongValue(t *testing.T) {
	const seed = 98562314
	t.Parallel()
	rng := rand.New(rand.NewSource(int64(seed)))
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(
		asyncOptions(rng.Int(),
			sim.AddHonestParticipants(
				1,
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
				sim.UniformStoragePower(gpbft.NewStoragePower(1))),
			sim.WithBaseChain(baseChain),
			sim.WithAdversary(adversary.NewImmediateDecideGenerator(
				adversaryValue,
				gpbft.NewStoragePower(3),
				// Justify the base-chain, but attempt to decide on something else.
				adversary.ImmediateDecideWithJustifiedValue(baseChain))),
		)...)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	require.ErrorContains(t, err, "has justification for a different value")
}

func TestIllegalCommittee_WrongSupplementalData(t *testing.T) {
	const seed = 98562314
	t.Parallel()
	rng := rand.New(rand.NewSource(int64(seed)))
	tsg := sim.NewTipSetGenerator(tipSetGeneratorSeed)
	baseChain := generateECChain(t, tsg)
	adversaryValue := baseChain.Extend(tsg.Sample())
	sm, err := sim.NewSimulation(
		asyncOptions(rng.Int(),
			sim.AddHonestParticipants(
				1,
				sim.NewUniformECChainGenerator(rng.Uint64(), 1, 5),
				sim.UniformStoragePower(gpbft.NewStoragePower(1))),
			sim.WithBaseChain(baseChain),
			sim.WithAdversary(adversary.NewImmediateDecideGenerator(
				adversaryValue,
				gpbft.NewStoragePower(3),
				// Justify the base-chain, but attempt to decide on something else.
				adversary.ImmediateDecideWithJustifiedSupplementalData(gpbft.SupplementalData{
					PowerTable: gpbft.MakeCid([]byte("wrong")),
				}))))...)
	require.NoError(t, err)

	err = sm.Run(1, maxRounds)
	require.ErrorContains(t, err, "message and justification have inconsistent supplemental data")
}
