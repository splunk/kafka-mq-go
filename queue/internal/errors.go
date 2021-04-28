package internal

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

var (
	// The list of errors returned by the confluent Kafka client that require reconnection
	errsRequiringReconnect = map[kafka.ErrorCode]bool{
		kafka.ErrMaxPollExceeded: true,
	}

	// The list of errors returned by the confluent Kafka client that indicate a timeout
	timeoutErrs = map[kafka.ErrorCode]bool{
		kafka.ErrTimedOut: true,
	}
)

// IsReconnectRequired returns true if the error specified requires the Kafka client to be reconnected
func IsReconnectRequired(err error) bool {
	kafkaError, ok := errors.Cause(err).(kafka.Error)
	if !ok {
		return false
	}

	// Reconnect is required if the error is one of the known codes or if the error is fatal.
	// An example where the error is fault but the consumer is unusable is `kafka.ErrMaxPollExceeded`
	if _, ok := errsRequiringReconnect[kafkaError.Code()]; ok || kafkaError.IsFatal() {
		return true
	}
	return false
}

// IsTimeoutError returns true if the error specified indicates a Kafka client timeout
func IsTimeoutError(err error) bool {
	kafkaError, ok := errors.Cause(err).(kafka.Error)
	if !ok {
		return false
	}

	if _, ok := timeoutErrs[kafkaError.Code()]; ok {
		return true
	}
	return false
}
