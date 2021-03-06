// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	proto "github.com/splunk/kafka-mq-go/queue/proto"
	mock "github.com/stretchr/testify/mock"
)

// Redeliverer is an autogenerated mock type for the Redeliverer type
type Redeliverer struct {
	mock.Mock
}

// Redeliver provides a mock function with given fields: markers
func (_m *Redeliverer) Redeliver(markers []*proto.Marker) {
	_m.Called(markers)
}

// Stop provides a mock function with given fields:
func (_m *Redeliverer) Stop() {
	_m.Called()
}
