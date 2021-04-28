// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	confluent_kafka_gokafka "github.com/confluentinc/confluent-kafka-go/kafka"

	mock "github.com/stretchr/testify/mock"

	time "time"
)

// Consumer is an autogenerated mock type for the Consumer type
type Consumer struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Consumer) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Commit provides a mock function with given fields:
func (_m *Consumer) Commit() ([]confluent_kafka_gokafka.TopicPartition, error) {
	ret := _m.Called()

	var r0 []confluent_kafka_gokafka.TopicPartition
	if rf, ok := ret.Get(0).(func() []confluent_kafka_gokafka.TopicPartition); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]confluent_kafka_gokafka.TopicPartition)
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

// CommitOffsets provides a mock function with given fields: offsets
func (_m *Consumer) CommitOffsets(offsets []confluent_kafka_gokafka.TopicPartition) ([]confluent_kafka_gokafka.TopicPartition, error) {
	ret := _m.Called(offsets)

	var r0 []confluent_kafka_gokafka.TopicPartition
	if rf, ok := ret.Get(0).(func([]confluent_kafka_gokafka.TopicPartition) []confluent_kafka_gokafka.TopicPartition); ok {
		r0 = rf(offsets)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]confluent_kafka_gokafka.TopicPartition)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func([]confluent_kafka_gokafka.TopicPartition) error); ok {
		r1 = rf(offsets)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadMessage provides a mock function with given fields: timeout
func (_m *Consumer) ReadMessage(timeout time.Duration) (*confluent_kafka_gokafka.Message, error) {
	ret := _m.Called(timeout)

	var r0 *confluent_kafka_gokafka.Message
	if rf, ok := ret.Get(0).(func(time.Duration) *confluent_kafka_gokafka.Message); ok {
		r0 = rf(timeout)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*confluent_kafka_gokafka.Message)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(time.Duration) error); ok {
		r1 = rf(timeout)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Subscribe provides a mock function with given fields: topic, rebalanceCb
func (_m *Consumer) Subscribe(topic string, rebalanceCb confluent_kafka_gokafka.RebalanceCb) error {
	ret := _m.Called(topic, rebalanceCb)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, confluent_kafka_gokafka.RebalanceCb) error); ok {
		r0 = rf(topic, rebalanceCb)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
