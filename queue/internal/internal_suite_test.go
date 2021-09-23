package internal_test

import (
	"testing"

	kafkaMocks "github.com/splunk/kafka-mq-go/pkg/kafka/mocks"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

func TestInternal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

const (
	messageTopic = "topic.message"
	markerTopic  = "topic.message"
	queueID      = "test-queue"
)

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

func mockProduce(kp *kafkaMocks.Producer, msg *confluentKafka.Message) *mock.Call {
	return kp.On("Produce", msg, mock.AnythingOfType("chan kafka.Event"))
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
