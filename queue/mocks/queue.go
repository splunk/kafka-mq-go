// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	queue "github.com/splunk/kafka-mq-go/queue"
	mock "github.com/stretchr/testify/mock"
)

// Queue is an autogenerated mock type for the Queue type
type Queue struct {
	mock.Mock
}

// Consumer provides a mock function with given fields:
func (_m *Queue) Consumer() (queue.Consumer, error) {
	ret := _m.Called()

	var r0 queue.Consumer
	if rf, ok := ret.Get(0).(func() queue.Consumer); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(queue.Consumer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Producer provides a mock function with given fields:
func (_m *Queue) Producer() (queue.Producer, error) {
	ret := _m.Called()

	var r0 queue.Producer
	if rf, ok := ret.Get(0).(func() queue.Producer); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(queue.Producer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
