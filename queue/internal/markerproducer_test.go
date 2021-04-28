package internal_test

import (
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka/mocks"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/pkg/testing/matchers"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal"
	kafkamq "cd.splunkdev.com/dferstay/kafka-mq-go/queue/proto"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("MarkerProducer", func() {
	var (
		err                error
		mp                 MarkerProducer
		kafkaClientFactory *mocks.ClientFactory
		kafkaProducer      *mocks.Producer
		criticalError      = errors.New("oops")
		redeliveryTimeout  = time.Duration(30 * time.Second)
		messages           = messageTopic
		msg                = &confluentKafka.Message{
			Key:   []byte(queueID),
			Value: []byte("test-value"),
			TopicPartition: confluentKafka.TopicPartition{
				Topic:     &messages,
				Partition: 0,
			},
		}
		mpConfig = MarkerProducerConfig{
			BootstrapServers: "1.2.3.4:9092",
			Topic:            markerTopic,
		}
	)

	BeforeEach(func() {
		err = nil
		kafkaClientFactory = &mocks.ClientFactory{}
		kafkaProducer = &mocks.Producer{}
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
		kafkaProducer.AssertExpectations(GinkgoT())
	})

	Describe("NewMarkerProducer", func() {
		JustBeforeEach(func() {
			mp, err = NewMarkerProducer(kafkaClientFactory, mpConfig)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			})
			It("should return a MqClient", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mp).ShouldNot(BeNil())
			})
		})

		When("Unable to create kafka producer", func() {
			BeforeEach(func() {
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mp).Should(BeNil())
			})
		})
	})

	Describe("SendStartMarker", func() {
		var (
			marker = kafkamq.Marker{
				Version: kafkamq.Marker_V1,
				Type:    kafkamq.Marker_START,
				MessageId: &kafkamq.MessageID{
					Partition: msg.TopicPartition.Partition,
					Offset:    int64(msg.TopicPartition.Offset),
				},
				RedeliverAfterMs: uint64(redeliveryTimeout / time.Millisecond),
				Key:              msg.Key,
				Value:            msg.Value,
			}
		)
		BeforeEach(func() {
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			mp, err = NewMarkerProducer(kafkaClientFactory, mpConfig)
		})
		JustBeforeEach(func() {
			err = mp.SendStartMarker(queueID, msg, redeliveryTimeout)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				data, err := marker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mpConfig.Topic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)
			})
			It("should produce a start marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		When("Unable to produce start marker", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
		When("Unable to deliver start marker", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("SendKeepAliveMarker", func() {
		var (
			marker = kafkamq.Marker{
				Version: kafkamq.Marker_V1,
				Type:    kafkamq.Marker_KEEPALIVE,
				MessageId: &kafkamq.MessageID{
					Partition: msg.TopicPartition.Partition,
					Offset:    int64(msg.TopicPartition.Offset),
				},
				RedeliverAfterMs: uint64(redeliveryTimeout / time.Millisecond),
			}
		)
		BeforeEach(func() {
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			mp, err = NewMarkerProducer(kafkaClientFactory, mpConfig)
		})
		JustBeforeEach(func() {
			err = mp.SendKeepAliveMarker(queueID, msg, redeliveryTimeout)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				data, err := marker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mpConfig.Topic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)
			})
			It("should produce a keep-alive marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		When("Unable to produce keep-alive marker", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
		When("Unable to deliver keep-alive marker", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("SendEndMarker", func() {
		var (
			marker = kafkamq.Marker{
				Version: kafkamq.Marker_V1,
				Type:    kafkamq.Marker_END,
				MessageId: &kafkamq.MessageID{
					Partition: msg.TopicPartition.Partition,
					Offset:    int64(msg.TopicPartition.Offset),
				},
			}
		)
		BeforeEach(func() {
			kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
			mp, err = NewMarkerProducer(kafkaClientFactory, mpConfig)
		})
		JustBeforeEach(func() {
			err = mp.SendEndMarker(queueID, msg)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				data, err := marker.Marshal()
				Expect(err).Should(BeNil())
				Expect(data).ShouldNot(BeNil())

				expectedMsg := &confluentKafka.Message{
					TopicPartition: confluentKafka.TopicPartition{Topic: &mpConfig.Topic, Partition: confluentKafka.PartitionAny},
					Key:            []byte(queueID),
					Value:          data,
				}
				mockProduceMsgOK(kafkaProducer, expectedMsg)
			})
			It("should produce an end marker", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		When("Unable to produce end marker", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
		When("Unable to deliver end marker", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should propagate the error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("Close", func() {
		JustBeforeEach(func() {
			err = mp.Close()
		})
		Context("Happy path when not tainted", func() {
			BeforeEach(func() {
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)

				mp, err = NewMarkerProducer(kafkaClientFactory, mpConfig)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mp).ShouldNot(BeNil())

				kafkaProducer.On("Close", mock.Anything).Return().Once()
			})
			It("should close the marker producer", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
})
