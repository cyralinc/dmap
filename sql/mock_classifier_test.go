// Code generated by mockery v2.42.1. DO NOT EDIT.

package sql

import (
	context "context"

	classification "github.com/cyralinc/dmap/classification"

	mock "github.com/stretchr/testify/mock"
)

// MockClassifier is an autogenerated mock type for the Classifier type
type MockClassifier struct {
	mock.Mock
}

type MockClassifier_Expecter struct {
	mock *mock.Mock
}

func (_m *MockClassifier) EXPECT() *MockClassifier_Expecter {
	return &MockClassifier_Expecter{mock: &_m.Mock}
}

// Classify provides a mock function with given fields: ctx, input
func (_m *MockClassifier) Classify(ctx context.Context, input map[string]interface{}) (classification.Result, error) {
	ret := _m.Called(ctx, input)

	if len(ret) == 0 {
		panic("no return value specified for Classify")
	}

	var r0 classification.Result
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, map[string]interface{}) (classification.Result, error)); ok {
		return rf(ctx, input)
	}
	if rf, ok := ret.Get(0).(func(context.Context, map[string]interface{}) classification.Result); ok {
		r0 = rf(ctx, input)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(classification.Result)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, map[string]interface{}) error); ok {
		r1 = rf(ctx, input)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockClassifier_Classify_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Classify'
type MockClassifier_Classify_Call struct {
	*mock.Call
}

// Classify is a helper method to define mock.On call
//   - ctx context.Context
//   - input map[string]interface{}
func (_e *MockClassifier_Expecter) Classify(ctx interface{}, input interface{}) *MockClassifier_Classify_Call {
	return &MockClassifier_Classify_Call{Call: _e.mock.On("Classify", ctx, input)}
}

func (_c *MockClassifier_Classify_Call) Run(run func(ctx context.Context, input map[string]interface{})) *MockClassifier_Classify_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(map[string]interface{}))
	})
	return _c
}

func (_c *MockClassifier_Classify_Call) Return(_a0 classification.Result, _a1 error) *MockClassifier_Classify_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockClassifier_Classify_Call) RunAndReturn(run func(context.Context, map[string]interface{}) (classification.Result, error)) *MockClassifier_Classify_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockClassifier creates a new instance of MockClassifier. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockClassifier(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockClassifier {
	mock := &MockClassifier{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}