package redelivery_test

import (
	"testing"
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	kafkamq "cd.splunkdev.com/dferstay/kafka-mq-go/queue/proto"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var (
	messageTopic = "markers-topic"
	markerTopic  = "markers-topic"
	validOpts    = queue.Config{
		BootstrapServers:              "1.2.3.4:9092",
		MessageTopic:                  messageTopic,
		MarkerTopic:                   markerTopic,
		RedeliveryTimeout:             30 * time.Minute,
		ConsumerReceiveBufferCapacity: 10,
		ConsumerNextMessageTimeout:    100 * time.Millisecond,
		ConsumerMaxMessagesPerCommit:  500,
		RedeliveryTracker: queue.RedeliveryTrackerConfig{
			UseNowIfNoMarkerSeen: 5 * time.Minute,
			NumOffsetsPerCommit:  500,
		},
	}
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redelivery Test Suite")
}

func createMessage(partition int, offset int, key []byte, value []byte) *confluentKafka.Message {
	timestamp := time.Time{}
	return createMessageWithTimestamp(partition, offset, timestamp, key, value)
}

func createMessageWithTimestamp(partition int, offset int, timestamp time.Time, key []byte, value []byte) *confluentKafka.Message {
	return &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{
			Partition: int32(partition),
			Offset:    confluentKafka.Offset(offset),
		},
		Value:     value,
		Key:       key,
		Timestamp: timestamp,
	}
}

func newStartMarker(offset int64, redeliverAfterMs uint64) *kafkamq.Marker {
	return &kafkamq.Marker{
		Version: kafkamq.Marker_V1,
		Type:    kafkamq.Marker_START,
		MessageId: &kafkamq.MessageID{
			Partition: 0,
			Offset:    offset,
		},
		RedeliverAfterMs: redeliverAfterMs,
		Key:              []byte("test key"),
		Value:            []byte("test value"),
	}
}

func newKeepAliveMarker(offset int64, redeliverAfterMs uint64) *kafkamq.Marker {
	// keep-alive has RedeliverAfterMS, but no Key/Value
	return &kafkamq.Marker{
		Version: kafkamq.Marker_V1,
		Type:    kafkamq.Marker_KEEPALIVE,
		MessageId: &kafkamq.MessageID{
			Partition: 0,
			Offset:    offset,
		},
		RedeliverAfterMs: redeliverAfterMs,
	}
}

func newEndMarker(offset int64) *kafkamq.Marker {
	// end doesn't have RedeliverAfterMS, or Key/Value
	return &kafkamq.Marker{
		Version: kafkamq.Marker_V1,
		Type:    kafkamq.Marker_END,
		MessageId: &kafkamq.MessageID{
			Partition: 0,
			Offset:    offset,
		},
	}
}

func methodWasCalled(m *mock.Mock, methodName string, expected ...interface{}) bool {
	for _, call := range m.Calls {
		if call.Method == methodName {
			_, differences := mock.Arguments(expected).Diff(call.Arguments)
			if differences == 0 {
				return true
			}
		}
	}
	return false
}
