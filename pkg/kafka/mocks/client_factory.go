// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	mock "github.com/stretchr/testify/mock"

	pkgkafka "github.com/splunk/kafka-mq-go/pkg/kafka"
)

// ClientFactory is an autogenerated mock type for the ClientFactory type
type ClientFactory struct {
	mock.Mock
}

// NewConsumer provides a mock function with given fields: config
func (_m *ClientFactory) NewConsumer(config kafka.ConfigMap) (pkgkafka.Consumer, error) {
	ret := _m.Called(config)

	var r0 pkgkafka.Consumer
	if rf, ok := ret.Get(0).(func(kafka.ConfigMap) pkgkafka.Consumer); ok {
		r0 = rf(config)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pkgkafka.Consumer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(kafka.ConfigMap) error); ok {
		r1 = rf(config)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewProducer provides a mock function with given fields: config
func (_m *ClientFactory) NewProducer(config kafka.ConfigMap) (pkgkafka.Producer, error) {
	ret := _m.Called(config)

	var r0 pkgkafka.Producer
	if rf, ok := ret.Get(0).(func(kafka.ConfigMap) pkgkafka.Producer); ok {
		r0 = rf(config)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pkgkafka.Producer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(kafka.ConfigMap) error); ok {
		r1 = rf(config)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
