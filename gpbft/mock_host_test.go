// Code generated by mockery v2.43.1. DO NOT EDIT.

package gpbft

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// MockHost is an autogenerated mock type for the Host type
type MockHost struct {
	mock.Mock
}

type MockHost_Expecter struct {
	mock *mock.Mock
}

func (_m *MockHost) EXPECT() *MockHost_Expecter {
	return &MockHost_Expecter{mock: &_m.Mock}
}

// Aggregate provides a mock function with given fields: pubKeys
func (_m *MockHost) Aggregate(pubKeys []PubKey) (Aggregate, error) {
	ret := _m.Called(pubKeys)

	if len(ret) == 0 {
		panic("no return value specified for Aggregate")
	}

	var r0 Aggregate
	var r1 error
	if rf, ok := ret.Get(0).(func([]PubKey) (Aggregate, error)); ok {
		return rf(pubKeys)
	}
	if rf, ok := ret.Get(0).(func([]PubKey) Aggregate); ok {
		r0 = rf(pubKeys)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(Aggregate)
		}
	}

	if rf, ok := ret.Get(1).(func([]PubKey) error); ok {
		r1 = rf(pubKeys)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockHost_Aggregate_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Aggregate'
type MockHost_Aggregate_Call struct {
	*mock.Call
}

// Aggregate is a helper method to define mock.On call
//   - pubKeys []PubKey
func (_e *MockHost_Expecter) Aggregate(pubKeys interface{}) *MockHost_Aggregate_Call {
	return &MockHost_Aggregate_Call{Call: _e.mock.On("Aggregate", pubKeys)}
}

func (_c *MockHost_Aggregate_Call) Run(run func(pubKeys []PubKey)) *MockHost_Aggregate_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]PubKey))
	})
	return _c
}

func (_c *MockHost_Aggregate_Call) Return(_a0 Aggregate, _a1 error) *MockHost_Aggregate_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockHost_Aggregate_Call) RunAndReturn(run func([]PubKey) (Aggregate, error)) *MockHost_Aggregate_Call {
	_c.Call.Return(run)
	return _c
}

// GetCommittee provides a mock function with given fields: instance
func (_m *MockHost) GetCommittee(instance uint64) (*Committee, error) {
	ret := _m.Called(instance)

	if len(ret) == 0 {
		panic("no return value specified for GetCommittee")
	}

	var r0 *Committee
	var r1 error
	if rf, ok := ret.Get(0).(func(uint64) (*Committee, error)); ok {
		return rf(instance)
	}
	if rf, ok := ret.Get(0).(func(uint64) *Committee); ok {
		r0 = rf(instance)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*Committee)
		}
	}

	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(instance)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockHost_GetCommittee_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCommittee'
type MockHost_GetCommittee_Call struct {
	*mock.Call
}

// GetCommittee is a helper method to define mock.On call
//   - instance uint64
func (_e *MockHost_Expecter) GetCommittee(instance interface{}) *MockHost_GetCommittee_Call {
	return &MockHost_GetCommittee_Call{Call: _e.mock.On("GetCommittee", instance)}
}

func (_c *MockHost_GetCommittee_Call) Run(run func(instance uint64)) *MockHost_GetCommittee_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *MockHost_GetCommittee_Call) Return(_a0 *Committee, _a1 error) *MockHost_GetCommittee_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockHost_GetCommittee_Call) RunAndReturn(run func(uint64) (*Committee, error)) *MockHost_GetCommittee_Call {
	_c.Call.Return(run)
	return _c
}

// GetProposal provides a mock function with given fields: instance
func (_m *MockHost) GetProposal(instance uint64) (*SupplementalData, ECChain, error) {
	ret := _m.Called(instance)

	if len(ret) == 0 {
		panic("no return value specified for GetProposal")
	}

	var r0 *SupplementalData
	var r1 ECChain
	var r2 error
	if rf, ok := ret.Get(0).(func(uint64) (*SupplementalData, ECChain, error)); ok {
		return rf(instance)
	}
	if rf, ok := ret.Get(0).(func(uint64) *SupplementalData); ok {
		r0 = rf(instance)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*SupplementalData)
		}
	}

	if rf, ok := ret.Get(1).(func(uint64) ECChain); ok {
		r1 = rf(instance)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(ECChain)
		}
	}

	if rf, ok := ret.Get(2).(func(uint64) error); ok {
		r2 = rf(instance)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// MockHost_GetProposal_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetProposal'
type MockHost_GetProposal_Call struct {
	*mock.Call
}

// GetProposal is a helper method to define mock.On call
//   - instance uint64
func (_e *MockHost_Expecter) GetProposal(instance interface{}) *MockHost_GetProposal_Call {
	return &MockHost_GetProposal_Call{Call: _e.mock.On("GetProposal", instance)}
}

func (_c *MockHost_GetProposal_Call) Run(run func(instance uint64)) *MockHost_GetProposal_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *MockHost_GetProposal_Call) Return(data *SupplementalData, chain ECChain, err error) *MockHost_GetProposal_Call {
	_c.Call.Return(data, chain, err)
	return _c
}

func (_c *MockHost_GetProposal_Call) RunAndReturn(run func(uint64) (*SupplementalData, ECChain, error)) *MockHost_GetProposal_Call {
	_c.Call.Return(run)
	return _c
}

// MarshalPayloadForSigning provides a mock function with given fields: _a0, _a1
func (_m *MockHost) MarshalPayloadForSigning(_a0 NetworkName, _a1 *Payload) []byte {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for MarshalPayloadForSigning")
	}

	var r0 []byte
	if rf, ok := ret.Get(0).(func(NetworkName, *Payload) []byte); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// MockHost_MarshalPayloadForSigning_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'MarshalPayloadForSigning'
type MockHost_MarshalPayloadForSigning_Call struct {
	*mock.Call
}

// MarshalPayloadForSigning is a helper method to define mock.On call
//   - _a0 NetworkName
//   - _a1 *Payload
func (_e *MockHost_Expecter) MarshalPayloadForSigning(_a0 interface{}, _a1 interface{}) *MockHost_MarshalPayloadForSigning_Call {
	return &MockHost_MarshalPayloadForSigning_Call{Call: _e.mock.On("MarshalPayloadForSigning", _a0, _a1)}
}

func (_c *MockHost_MarshalPayloadForSigning_Call) Run(run func(_a0 NetworkName, _a1 *Payload)) *MockHost_MarshalPayloadForSigning_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(NetworkName), args[1].(*Payload))
	})
	return _c
}

func (_c *MockHost_MarshalPayloadForSigning_Call) Return(_a0 []byte) *MockHost_MarshalPayloadForSigning_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_MarshalPayloadForSigning_Call) RunAndReturn(run func(NetworkName, *Payload) []byte) *MockHost_MarshalPayloadForSigning_Call {
	_c.Call.Return(run)
	return _c
}

// NetworkName provides a mock function with given fields:
func (_m *MockHost) NetworkName() NetworkName {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for NetworkName")
	}

	var r0 NetworkName
	if rf, ok := ret.Get(0).(func() NetworkName); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(NetworkName)
	}

	return r0
}

// MockHost_NetworkName_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'NetworkName'
type MockHost_NetworkName_Call struct {
	*mock.Call
}

// NetworkName is a helper method to define mock.On call
func (_e *MockHost_Expecter) NetworkName() *MockHost_NetworkName_Call {
	return &MockHost_NetworkName_Call{Call: _e.mock.On("NetworkName")}
}

func (_c *MockHost_NetworkName_Call) Run(run func()) *MockHost_NetworkName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockHost_NetworkName_Call) Return(_a0 NetworkName) *MockHost_NetworkName_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_NetworkName_Call) RunAndReturn(run func() NetworkName) *MockHost_NetworkName_Call {
	_c.Call.Return(run)
	return _c
}

// ReceiveDecision provides a mock function with given fields: decision
func (_m *MockHost) ReceiveDecision(decision *Justification) (time.Time, error) {
	ret := _m.Called(decision)

	if len(ret) == 0 {
		panic("no return value specified for ReceiveDecision")
	}

	var r0 time.Time
	var r1 error
	if rf, ok := ret.Get(0).(func(*Justification) (time.Time, error)); ok {
		return rf(decision)
	}
	if rf, ok := ret.Get(0).(func(*Justification) time.Time); ok {
		r0 = rf(decision)
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	if rf, ok := ret.Get(1).(func(*Justification) error); ok {
		r1 = rf(decision)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockHost_ReceiveDecision_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReceiveDecision'
type MockHost_ReceiveDecision_Call struct {
	*mock.Call
}

// ReceiveDecision is a helper method to define mock.On call
//   - decision *Justification
func (_e *MockHost_Expecter) ReceiveDecision(decision interface{}) *MockHost_ReceiveDecision_Call {
	return &MockHost_ReceiveDecision_Call{Call: _e.mock.On("ReceiveDecision", decision)}
}

func (_c *MockHost_ReceiveDecision_Call) Run(run func(decision *Justification)) *MockHost_ReceiveDecision_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*Justification))
	})
	return _c
}

func (_c *MockHost_ReceiveDecision_Call) Return(_a0 time.Time, _a1 error) *MockHost_ReceiveDecision_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockHost_ReceiveDecision_Call) RunAndReturn(run func(*Justification) (time.Time, error)) *MockHost_ReceiveDecision_Call {
	_c.Call.Return(run)
	return _c
}

// RequestBroadcast provides a mock function with given fields: mb
func (_m *MockHost) RequestBroadcast(mb *MessageBuilder) error {
	ret := _m.Called(mb)

	if len(ret) == 0 {
		panic("no return value specified for RequestBroadcast")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*MessageBuilder) error); ok {
		r0 = rf(mb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockHost_RequestBroadcast_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RequestBroadcast'
type MockHost_RequestBroadcast_Call struct {
	*mock.Call
}

// RequestBroadcast is a helper method to define mock.On call
//   - mb *MessageBuilder
func (_e *MockHost_Expecter) RequestBroadcast(mb interface{}) *MockHost_RequestBroadcast_Call {
	return &MockHost_RequestBroadcast_Call{Call: _e.mock.On("RequestBroadcast", mb)}
}

func (_c *MockHost_RequestBroadcast_Call) Run(run func(mb *MessageBuilder)) *MockHost_RequestBroadcast_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*MessageBuilder))
	})
	return _c
}

func (_c *MockHost_RequestBroadcast_Call) Return(_a0 error) *MockHost_RequestBroadcast_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_RequestBroadcast_Call) RunAndReturn(run func(*MessageBuilder) error) *MockHost_RequestBroadcast_Call {
	_c.Call.Return(run)
	return _c
}

// RequestRebroadcast provides a mock function with given fields: instant
func (_m *MockHost) RequestRebroadcast(instant Instant) error {
	ret := _m.Called(instant)

	if len(ret) == 0 {
		panic("no return value specified for RequestRebroadcast")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(Instant) error); ok {
		r0 = rf(instant)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockHost_RequestRebroadcast_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RequestRebroadcast'
type MockHost_RequestRebroadcast_Call struct {
	*mock.Call
}

// RequestRebroadcast is a helper method to define mock.On call
//   - instant Instant
func (_e *MockHost_Expecter) RequestRebroadcast(instant interface{}) *MockHost_RequestRebroadcast_Call {
	return &MockHost_RequestRebroadcast_Call{Call: _e.mock.On("RequestRebroadcast", instant)}
}

func (_c *MockHost_RequestRebroadcast_Call) Run(run func(instant Instant)) *MockHost_RequestRebroadcast_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(Instant))
	})
	return _c
}

func (_c *MockHost_RequestRebroadcast_Call) Return(_a0 error) *MockHost_RequestRebroadcast_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_RequestRebroadcast_Call) RunAndReturn(run func(Instant) error) *MockHost_RequestRebroadcast_Call {
	_c.Call.Return(run)
	return _c
}

// SetAlarm provides a mock function with given fields: at
func (_m *MockHost) SetAlarm(at time.Time) {
	_m.Called(at)
}

// MockHost_SetAlarm_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetAlarm'
type MockHost_SetAlarm_Call struct {
	*mock.Call
}

// SetAlarm is a helper method to define mock.On call
//   - at time.Time
func (_e *MockHost_Expecter) SetAlarm(at interface{}) *MockHost_SetAlarm_Call {
	return &MockHost_SetAlarm_Call{Call: _e.mock.On("SetAlarm", at)}
}

func (_c *MockHost_SetAlarm_Call) Run(run func(at time.Time)) *MockHost_SetAlarm_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(time.Time))
	})
	return _c
}

func (_c *MockHost_SetAlarm_Call) Return() *MockHost_SetAlarm_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockHost_SetAlarm_Call) RunAndReturn(run func(time.Time)) *MockHost_SetAlarm_Call {
	_c.Call.Return(run)
	return _c
}

// Time provides a mock function with given fields:
func (_m *MockHost) Time() time.Time {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Time")
	}

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

// MockHost_Time_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Time'
type MockHost_Time_Call struct {
	*mock.Call
}

// Time is a helper method to define mock.On call
func (_e *MockHost_Expecter) Time() *MockHost_Time_Call {
	return &MockHost_Time_Call{Call: _e.mock.On("Time")}
}

func (_c *MockHost_Time_Call) Run(run func()) *MockHost_Time_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockHost_Time_Call) Return(_a0 time.Time) *MockHost_Time_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_Time_Call) RunAndReturn(run func() time.Time) *MockHost_Time_Call {
	_c.Call.Return(run)
	return _c
}

// Verify provides a mock function with given fields: pubKey, msg, sig
func (_m *MockHost) Verify(pubKey PubKey, msg []byte, sig []byte) error {
	ret := _m.Called(pubKey, msg, sig)

	if len(ret) == 0 {
		panic("no return value specified for Verify")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(PubKey, []byte, []byte) error); ok {
		r0 = rf(pubKey, msg, sig)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockHost_Verify_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Verify'
type MockHost_Verify_Call struct {
	*mock.Call
}

// Verify is a helper method to define mock.On call
//   - pubKey PubKey
//   - msg []byte
//   - sig []byte
func (_e *MockHost_Expecter) Verify(pubKey interface{}, msg interface{}, sig interface{}) *MockHost_Verify_Call {
	return &MockHost_Verify_Call{Call: _e.mock.On("Verify", pubKey, msg, sig)}
}

func (_c *MockHost_Verify_Call) Run(run func(pubKey PubKey, msg []byte, sig []byte)) *MockHost_Verify_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(PubKey), args[1].([]byte), args[2].([]byte))
	})
	return _c
}

func (_c *MockHost_Verify_Call) Return(_a0 error) *MockHost_Verify_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockHost_Verify_Call) RunAndReturn(run func(PubKey, []byte, []byte) error) *MockHost_Verify_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockHost creates a new instance of MockHost. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockHost(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockHost {
	mock := &MockHost{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
