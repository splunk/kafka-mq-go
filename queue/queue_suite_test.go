package queue_test

import (
	"testing"
	"time"

	kafkaMocks "cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka/mocks"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

const (
	messageTopic = "topic.message"
	markerTopic  = "topic.message"
	queueID      = "test-queue"
)

var validOpts = queue.Config{
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

func TestKafka(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kafka Queue Implementation Suite")
}

func mockProduceOK(kp *kafkaMocks.Producer) *mock.Call {
	t := messageTopic
	deliveryMsg := &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{
			Topic:     &t,
			Partition: 0,
			Offset:    0,
		},
	}
	produceCall := mockProduceAny(kp)
	return mockDeliveryEvent(produceCall, deliveryMsg)
}

func mockProduceMsgOK(kp *kafkaMocks.Producer, msg *confluentKafka.Message) *mock.Call {
	t := messageTopic
	deliveryMsg := &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{
			Topic:     &t,
			Partition: 0,
			Offset:    0,
		},
	}
	produceCall := mockProduce(kp, msg)
	return mockDeliveryEvent(produceCall, deliveryMsg)
}

func mockProduceDeliveryFail(kp *kafkaMocks.Producer) *mock.Call {
	deliveryMsg := &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{
			Error: errors.New("mock kafka delivery failure"),
		},
	}
	produceCall := mockProduceAny(kp)
	return mockDeliveryEvent(produceCall, deliveryMsg)
}

func mockProduceFail(kp *kafkaMocks.Producer) *mock.Call {
	return mockProduceFailWithError(kp, errors.New("mock kafka production failure"))
}

func mockProduceFailWithError(kp *kafkaMocks.Producer, err error) *mock.Call {
	return mockProduceAny(kp).Return(err)
}

func mockDeliveryEvent(produceCall *mock.Call, deliveryMsg *confluentKafka.Message) *mock.Call {
	return produceCall.Return(nil).Run(func(args mock.Arguments) {
		deliveryChan := args.Get(1).(chan confluentKafka.Event)

		go func() {
			deliveryChan <- deliveryMsg
		}()
	})
}

func mockProduceAny(kp *kafkaMocks.Producer) *mock.Call {
	return kp.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.AnythingOfType("chan kafka.Event"))
}

func mockProduce(kp *kafkaMocks.Producer, msg *confluentKafka.Message) *mock.Call {
	return kp.On("Produce", msg, mock.AnythingOfType("chan kafka.Event"))
}

// Can be used to mock out calls to the KafkaConsumer and KafkaProducer
func mockConsumerLoopMayRun(kc *kafkaMocks.Consumer, kp *kafkaMocks.Producer) {
	// NB: We set the Key of the message such that it will not be filtered by the mqclient.NextMessage() call in the consumer loop.
	//     This is important, because otherwise mqclient.NextMessage() will spin in a tight loop reading messages from kafka that
	//     it will end up filtering.  For messages that are not filtered, the consumer loop will block after the internal buffered
	//     channel is full.
	kc.On("ReadMessage", mock.Anything).Return(&confluentKafka.Message{Key: []byte(queueID)}, nil).Maybe()
	kc.On("Commit").Return([]confluentKafka.TopicPartition{}, nil).Maybe()
	mockProduceOK(kp).Maybe()
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
