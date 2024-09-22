package gpbft

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	_ CommitteeProvider = (*mockCommitteeProvider)(nil)
)

type mockCommitteeProvider struct {
	mock.Mock
}

func (m *mockCommitteeProvider) GetCommittee(instance uint64) (*Committee, error) {
	args := m.Called(instance)
	if committee, ok := args.Get(0).(*Committee); ok {
		return committee, args.Error(1)
	}
	return nil, args.Error(1)
}

func TestCachedCommitteeProvider_GetCommittee(t *testing.T) {
	var (
		instance1                    = uint64(1)
		instance2                    = uint64(2)
		instance3                    = uint64(3)
		instance5                    = uint64(5)
		instance6                    = uint64(6)
		instance7                    = uint64(7)
		committeeWithValidPowerTable = &Committee{
			PowerTable: generateValidPowerTable(t),
			Beacon:     []byte("fish")}
		committee5 = &Committee{
			PowerTable: generateValidPowerTable(t),
			Beacon:     []byte("fish"),
		}
		committee6 = &Committee{
			PowerTable: generateValidPowerTable(t),
			Beacon:     []byte("fish"),
		}
		committee7 = &Committee{
			PowerTable: generateValidPowerTable(t),
			Beacon:     []byte("fish"),
		}

		mockDelegate = new(mockCommitteeProvider)
		subject      = newCachedCommitteeProvider(mockDelegate)
	)

	mockDelegate.On("GetCommittee", instance1).Return(committeeWithValidPowerTable, nil)
	t.Run("delegates cache miss", func(t *testing.T) {
		result, err := subject.GetCommittee(1)
		require.NoError(t, err)
		require.Equal(t, committeeWithValidPowerTable, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance1)
	})
	t.Run("caches", func(t *testing.T) {
		result, err := subject.GetCommittee(1)
		require.NoError(t, err)
		require.Equal(t, committeeWithValidPowerTable, result)
		mockDelegate.AssertNotCalled(t, "GetCommittee")
	})
	t.Run("delegates error", func(t *testing.T) {
		wantErr := errors.New("undadasea")
		mockDelegate.On("GetCommittee", instance2).Return(nil, wantErr)
		result, err := subject.GetCommittee(instance2)
		require.ErrorIs(t, err, ErrValidationNoCommittee)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance2)
	})
	t.Run("checks nil committee", func(t *testing.T) {
		mockDelegate.On("GetCommittee", instance3).Return(nil, nil)
		result, err := subject.GetCommittee(instance3)
		require.ErrorContains(t, err, "unexpected")
		require.Nil(t, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance3)
	})
	t.Run("evicts instances before given", func(t *testing.T) {
		mockDelegate.On("GetCommittee", instance5).Return(committee5, nil)
		mockDelegate.On("GetCommittee", instance6).Return(committee6, nil)
		mockDelegate.On("GetCommittee", instance7).Return(committee7, nil)

		// Populate
		result, err := subject.GetCommittee(instance5)
		require.NoError(t, err)
		require.Equal(t, committee5, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance5)
		result, err = subject.GetCommittee(instance6)
		require.NoError(t, err)
		require.Equal(t, committee6, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance6)
		result, err = subject.GetCommittee(instance7)
		require.NoError(t, err)
		require.Equal(t, committee7, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance7)

		// Assert cache hit.
		result, err = subject.GetCommittee(instance5)
		require.NoError(t, err)
		require.Equal(t, committee5, result)
		mockDelegate.AssertNotCalled(t, "GetCommittee")
		result, err = subject.GetCommittee(instance6)
		require.NoError(t, err)
		require.Equal(t, committee6, result)
		mockDelegate.AssertNotCalled(t, "GetCommittee")
		result, err = subject.GetCommittee(instance7)
		require.NoError(t, err)
		require.Equal(t, committee7, result)
		mockDelegate.AssertNotCalled(t, "GetCommittee")

		// Evict committees prior to 6.
		subject.EvictCommitteesBefore(instance6)

		// Assert cache miss.
		result, err = subject.GetCommittee(instance5)
		require.NoError(t, err)
		require.Equal(t, committee5, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance5)
		result, err = subject.GetCommittee(instance1)
		require.NoError(t, err)
		require.Equal(t, committeeWithValidPowerTable, result)
		mockDelegate.AssertCalled(t, "GetCommittee", instance1)
	})
}

func generateValidPowerTable(t *testing.T) *PowerTable {
	pt := NewPowerTable()
	require.NoError(t, pt.Add(PowerEntry{
		ID:     ActorID(rand.Uint64N(100)),
		Power:  NewStoragePower(int64(rand.Uint64N(100))),
		PubKey: []byte("lobster"),
	}))
	return pt
}
