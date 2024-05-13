package sim

import "github.com/filecoin-project/go-f3/gpbft"

type StoragePowerGenerator func(instance uint64, id gpbft.ActorID) *gpbft.StoragePower

func UniformStoragePower(power *gpbft.StoragePower) StoragePowerGenerator {
	return func(uint64, gpbft.ActorID) *gpbft.StoragePower {
		return power
	}
}
