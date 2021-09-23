package internal_test

import (
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka/mocks"
	. "github.com/splunk/kafka-mq-go/pkg/testing/matchers"
	. "github.com/splunk/kafka-mq-go/queue/internal"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

const (
	mqNextMessageTimeout = 100 * time.Millisecond
)

var (
	mqConfig = MqClientConfig{
		BootstrapServers:     "1.2.3.4:9092",
		MessageTopic:         "messages",
		MarkerTopic:          "markers",
		RedeliveryTimeout:    time.Duration(30 * time.Second),
		MaxMessagesPerCommit: 500,
	}
	consumerMaxPollExceededError = confluentKafka.NewError(confluentKafka.ErrMaxPollExceeded, "max poll exceeded, leaving consumer group", false)
	criticalError                = errors.New("oops")
	fatalConsumerError           = confluentKafka.NewError(confluentKafka.ErrFatal, "a fatal error", true)
)

var _ = Describe("MqClient", func() {
	var (
		err                error
		mq                 MqClient
		kafkaClientFactory *mocks.ClientFactory
		kafkaProducer      *mocks.Producer
		kafkaConsumer      *mocks.Consumer
	)

	BeforeEach(func() {
		err = nil
		kafkaClientFactory = &mocks.ClientFactory{}
		kafkaProducer = &mocks.Producer{}
		kafkaConsumer = &mocks.Consumer{}
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
		kafkaProducer.AssertExpectations(GinkgoT())
		kafkaConsumer.AssertExpectations(GinkgoT())
	})

	Describe("NewMqClient", func() {
		JustBeforeEach(func() {
			mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			})
			It("should return a MqClient", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
			})
		})

		When("Unable to create kafka consumer", func() {
			BeforeEach(func() {
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mq).Should(BeNil())
			})
		})

		When("Unable to create kafka producer", func() {
			BeforeEach(func() {
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mq).Should(BeNil())
			})
		})

		When("Consumer unable to subscribe to topic", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(criticalError)
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mq).Should(BeNil())
			})
		})
	})

	Describe("NextMessage", func() {
		var (
			msg         *confluentKafka.Message
			expectedMsg = &confluentKafka.Message{
				Key:   []byte(queueID),
				Value: []byte("test-value"),
				TopicPartition: confluentKafka.TopicPartition{
					Topic:     &mqConfig.MessageTopic,
					Partition: confluentKafka.PartitionAny,
				},
			}
		)

		BeforeEach(func() {
			kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
			kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

			mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mq).ShouldNot(BeNil())
		})

		JustBeforeEach(func() {
			msg, err = mq.NextMessage(mqNextMessageTimeout)
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				kafkaConsumer.On("Commit").Return([]confluentKafka.TopicPartition{}, nil)
				mockProduceOK(kafkaProducer)
			})
			It("should return the expected message", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(msg).ShouldNot(BeNil())

				// check that message content matches the expected
				Expect(msg).Should(Equal(expectedMsg))

				// assert that the client does not need resetting
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to read message", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, criticalError)
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(msg).Should(BeNil())

				// assert that the client does not need resetting
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to read message with consumer error that requires reset", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, consumerMaxPollExceededError)
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(consumerMaxPollExceededError))
				Expect(msg).Should(BeNil())

				// assert that the client needs to reconnect
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})

		When("Unable to read message with fatal consumer error", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(nil, fatalConsumerError)
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(fatalConsumerError))
				Expect(msg).Should(BeNil())

				// assert that the client needs to reconnect
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})

		When("Unable to produce start marker", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error and require resetting", func() {
				Expect(err).Should(HaveOccurred())
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeTrue())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to deliver start marker", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error and require resetting", func() {
				Expect(err).Should(HaveOccurred())
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeTrue())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to commit message offset", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				kafkaConsumer.On("Commit").Return(nil, criticalError)
				mockProduceOK(kafkaProducer)
			})
			It("should propagate the error and require resetting", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeTrue())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to commit message offset with fatal consumer error", func() {
			BeforeEach(func() {
				kafkaConsumer.On("ReadMessage", mock.Anything).Return(expectedMsg, nil)
				kafkaConsumer.On("Commit").Return(nil, fatalConsumerError)
				mockProduceOK(kafkaProducer)
			})
			It("should propagate the error and require reconnecting", func() {
				Expect(err).Should(BeCausedBy(fatalConsumerError))
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeTrue())
				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})

		When("Closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaProducer.On("Close").Return()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("NeedsReset", func() {
			BeforeEach(func() {
				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReset()).Should(BeTrue())
			})
		})

		When("NeedsReconnect", func() {
			BeforeEach(func() {
				mqClientNeedsReconnect(mq, kafkaConsumer)
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(msg).Should(BeNil())

				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})
	})

	Describe("ProcessingInProgress", func() {
		var (
			msg = &confluentKafka.Message{
				Key:   []byte(queueID),
				Value: []byte("test-value"),
				TopicPartition: confluentKafka.TopicPartition{
					Topic:     &mqConfig.MessageTopic,
					Partition: confluentKafka.PartitionAny,
				},
			}
			keepAliveMarker = kafkamq.Marker{
				Version: kafkamq.Marker_V1,
				Type:    kafkamq.Marker_KEEPALIVE,
				MessageId: &kafkamq.MessageID{
					Partition: msg.TopicPartition.Partition,
					Offset:    int64(msg.TopicPartition.Offset),
				},
				RedeliverAfterMs: uint64(mqConfig.RedeliveryTimeout / time.Millisecond),
			}
		)

		BeforeEach(func() {
			kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
			kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

			mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mq).ShouldNot(BeNil())
		})

		JustBeforeEach(func() {
			err = mq.ProcessingInProgress(msg)
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				data, err := keepAliveMarker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)
			})
			It("should produce a keep-alive marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to produce keep-alive marker", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to deliver keep-alive marker", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaProducer.On("Close").Return()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("NeedsReset", func() {
			BeforeEach(func() {
				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)

				data, err := keepAliveMarker.Marshal()
				Expect(data).ShouldNot(BeNil())
				Expect(err).Should(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)

			})
			It("should should produce a keep-alive marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeTrue())
			})
		})

		When("NeedsReconnect", func() {
			BeforeEach(func() {
				mqClientNeedsReconnect(mq, kafkaConsumer)

				data, err := keepAliveMarker.Marshal()
				Expect(data).ShouldNot(BeNil())
				Expect(err).Should(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)

			})
			It("should should produce a keep-alive marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})
	})

	Describe("Processed", func() {
		var (
			msg = &confluentKafka.Message{
				Key:   []byte(queueID),
				Value: []byte("test-value"),
				TopicPartition: confluentKafka.TopicPartition{
					Topic:     &mqConfig.MessageTopic,
					Partition: confluentKafka.PartitionAny,
				},
			}
			endMarker = kafkamq.Marker{
				Version: kafkamq.Marker_V1,
				Type:    kafkamq.Marker_END,
				MessageId: &kafkamq.MessageID{
					Partition: msg.TopicPartition.Partition,
					Offset:    int64(msg.TopicPartition.Offset),
				},
			}
		)

		BeforeEach(func() {
			kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
			kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

			mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mq).ShouldNot(BeNil())
		})

		JustBeforeEach(func() {
			err = mq.Processed(msg)
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				data, err := endMarker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)
			})
			It("should produce a end marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("Unable to produce end marker", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})

		When("Unable to deliver end marker", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})

		When("NeedsReset", func() {
			BeforeEach(func() {
				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)

				data, err := endMarker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)

			})
			It("should should produce a end marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeTrue())
			})
		})

		When("NeedsReconnect", func() {
			BeforeEach(func() {
				mqClientNeedsReconnect(mq, kafkaConsumer)

				data, err := endMarker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mqConfig.MarkerTopic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)

			})
			It("should should produce a end marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReconnect()).Should(BeTrue())
			})
		})

		When("Closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaProducer.On("Close").Return()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})
	})

	Describe("Reset", func() {

		JustBeforeEach(func() {
			err = mq.Reset()
		})

		Context("Happy path when no reset needed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Twice()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
			It("should reset the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})

		Context("Happy path when reset needed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Twice()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)
			})
			It("should reset the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})

		When("Consumer unable to subscribe to topic", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(criticalError).Once()
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})

		When("Closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				kafkaConsumer.On("Close").Return(nil)
				kafkaProducer.On("Close").Return()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})
	})

	Describe("Reconnect", func() {

		JustBeforeEach(func() {
			err = mq.Reconnect()
		})

		Context("Happy path when no reconnect needed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Twice()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
			It("should reconnect the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		Context("Happy path when reconnect needed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Twice()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReconnect()).Should(BeFalse())

				mqClientNeedsReconnect(mq, kafkaConsumer)
			})
			It("should reconnect the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq.NeedsReconnect()).Should(BeFalse())
			})
		})

		When("Unable to create consumer", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil).Once()
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReconnect()).Should(BeFalse())

				kafkaClientFactory.On("NewConsumer", mock.Anything).Once().Return(nil, criticalError)
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})

		When("Consumer unable to subscribe to topic", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Close").Return(nil)
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReconnect()).Should(BeFalse())

				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(criticalError).Once()
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})

		When("Closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReconnect()).Should(BeFalse())

				kafkaConsumer.On("Close").Return(nil)
				kafkaProducer.On("Close").Return()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit with an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(mq.NeedsReset()).Should(BeFalse())
			})
		})
	})

	Describe("Close", func() {

		JustBeforeEach(func() {
			err = mq.Close()
		})

		Context("Happy path when no reset needed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				kafkaProducer.On("Close", mock.Anything).Return().Once()
				kafkaConsumer.On("Close", mock.Anything).Return(nil).Once()
			})
			It("should close the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Happy path when needs resetting", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)

				kafkaProducer.On("Close", mock.Anything).Return().Once()
				kafkaConsumer.On("Close", mock.Anything).Return(nil).Once()
			})
			It("should close the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Happy path when needs reconnecting", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				mqClientNeedsReconnect(mq, kafkaConsumer)

				kafkaProducer.On("Close", mock.Anything).Return().Once()
				kafkaConsumer.On("Close", mock.Anything).Return(nil).Once()
			})
			It("should close the client", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("Unable to close kafka consumer", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				kafkaProducer.On("Close", mock.Anything).Return().Once()
				kafkaConsumer.On("Close", mock.Anything).Return(criticalError).Once()
			})
			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})

		When("mqclient is already closed", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil).Once()
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mq, err = NewMqClient(kafkaClientFactory, mqConfig, queueID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mq).ShouldNot(BeNil())
				Expect(mq.NeedsReset()).Should(BeFalse())

				mqClientNeedsReset(mq, kafkaConsumer, kafkaProducer)

				kafkaProducer.On("Close", mock.Anything).Return().Once()
				kafkaConsumer.On("Close", mock.Anything).Return(nil).Once()

				err = mq.Close()
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("should short-circuit without an error", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})

})

func mqClientNeedsReset(mq MqClient, kc *mocks.Consumer, kp *mocks.Producer) {
	msg := &confluentKafka.Message{
		Key:   []byte(queueID),
		Value: []byte("test-value"),
		TopicPartition: confluentKafka.TopicPartition{
			Topic:     &mqConfig.MessageTopic,
			Partition: confluentKafka.PartitionAny,
		},
	}

	kc.On("ReadMessage", mock.Anything).Return(msg, nil)
	kc.On("Commit").Return(nil, criticalError)
	mockProduceOK(kp)

	nilMsg, err := mq.NextMessage(mqNextMessageTimeout)
	Expect(err).Should(BeCausedBy(criticalError))
	Expect(nilMsg).Should(BeNil())

	Expect(mq.NeedsReset()).Should(BeTrue())
}

func mqClientNeedsReconnect(mq MqClient, kc *mocks.Consumer) {
	msg := &confluentKafka.Message{
		Key:   []byte(queueID),
		Value: []byte("test-value"),
		TopicPartition: confluentKafka.TopicPartition{
			Topic:     &mqConfig.MessageTopic,
			Partition: confluentKafka.PartitionAny,
		},
	}

	kc.On("ReadMessage", mock.Anything).Return(msg, consumerMaxPollExceededError)

	nilMsg, err := mq.NextMessage(mqNextMessageTimeout)
	Expect(err).Should(HaveOccurred())
	Expect(nilMsg).Should(BeNil())

	Expect(mq.NeedsReconnect()).Should(BeTrue())
}
