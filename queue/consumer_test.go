package queue_test

import (
	"context"
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka/mocks"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/pkg/testing/matchers"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("Consumer", func() {
	const (
		receiveTimeout = 1 * time.Second
	)
	var (
		q                  Queue
		c                  Consumer
		msg                Message
		err                error
		kafkaClientFactory *mocks.ClientFactory
		kafkaConsumer      *mocks.Consumer
		kafkaProducer      *mocks.Producer

		messageTopic = "messages"
		payload      = []byte("test message")
		expectedMsg  = &confluentKafka.Message{
			Key:   []byte(queueID),
			Value: payload,
			TopicPartition: confluentKafka.TopicPartition{
				Topic:     &messageTopic,
				Partition: confluentKafka.PartitionAny,
			},
		}
		criticalError = errors.New("oops")
		timeoutError  = confluentKafka.NewError(confluentKafka.ErrTimedOut, "a timeout", false)
	)

	BeforeEach(func() {
		kafkaClientFactory = &mocks.ClientFactory{}
		kafkaConsumer = &mocks.Consumer{}
		kafkaProducer = &mocks.Producer{}

		q = NewQueue(queueID, validOpts, kafkaClientFactory)
		Expect(q).ShouldNot(BeNil())

		kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
		kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
		kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
	})

	JustBeforeEach(func() {
		c, err = q.Consumer()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(c).ShouldNot(BeNil())
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
		kafkaConsumer.AssertExpectations(GinkgoT())
		kafkaProducer.AssertExpectations(GinkgoT())
	})

	Describe("ReceiveMessage", func() {
		JustBeforeEach(func() {
			ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
			defer cancel()
			msg, err = c.Receive(ctx)
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				// The background consumer loop will fill the receive buffer with the expected message
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				kafkaConsumer.On("Commit").Return([]confluentKafka.TopicPartition{}, nil)
				mockProduceOK(kafkaProducer)
			})
			It("should work", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(msg.Payload()).Should(Equal(expectedMsg.Value))
			})
		})

		Context("Timeout receiving message", func() {
			BeforeEach(func() {
				// The background consumer loop will fail to put any messages on the receive buffer
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, timeoutError)
			})
			It("should return not put a message in the internal buffer and not return an error", func() {
				Expect(err).Should(BeNil())
				Expect(msg).Should(BeNil())
			})
		})

	})

	Describe("Close", func() {
		BeforeEach(func() {
			// The consumer run loop runs in the background, and it may make calls to the kafka consumer and producer while it runs
			mockConsumerLoopMayRun(kafkaConsumer, kafkaProducer)

			kafkaProducer.On("Close").Return()
		})

		JustBeforeEach(func() {
			err = c.Close()
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
			})
			It("should close the kafka consumer", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("failure to close the consumer", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(criticalError)
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})
	})

	Describe("Operations on a closed consumer", func() {
		JustBeforeEach(func() {
			err = c.Close()
			Expect(err).ShouldNot(HaveOccurred())
		})
		Context("Receive on a closed consumer", func() {
			BeforeEach(func() {
				// If the background consumer loop runs, it will fail to put any messages on the receive buffer
				// and thus calls to receive message will fail to pull messages off of the internal
				// buffered channel after it is closed instead of reading message off of the channel.
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, criticalError).Maybe()

				kafkaProducer.On("Close").Return()
				kafkaConsumer.On("Close").Return(nil)
			})
			JustBeforeEach(func() {
				ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
				defer cancel()
				msg, err = c.Receive(ctx)
			})
			It("should return an error indicating that the consumer has been closed", func() {
				Expect(err).Should(MatchError(ContainSubstring("called after the consumer was closed")))
				Expect(msg).Should(BeNil())
			})
		})

		Context("Close on a closed consumer", func() {
			BeforeEach(func() {
				// If the background consumer loop runs, it will fail to put any messages on the receive buffer
				// and thus calls to receive message will fail to pull messages off of the internal
				// buffered channel after it is closed instead of reading message off of the channel.
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, criticalError).Maybe()

				kafkaProducer.On("Close").Return()
				kafkaConsumer.On("Close").Return(nil)
			})
			JustBeforeEach(func() {
				err = c.Close()
			})
			It("should succeed", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
})
