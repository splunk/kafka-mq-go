// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	redelivery "cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"
	mock "github.com/stretchr/testify/mock"
)

// OffsetCommitter is an autogenerated mock type for the OffsetCommitter type
type OffsetCommitter struct {
	mock.Mock
}

// AddMarker provides a mock function with given fields: _a0
func (_m *OffsetCommitter) AddMarker(_a0 redelivery.RecordID) {
	_m.Called(_a0)
}

// DropOffsets provides a mock function with given fields: partition
func (_m *OffsetCommitter) DropOffsets(partition int32) {
	_m.Called(partition)
}
