package queue_test

import (
	"github.com/splunk/kafka-mq-go/pkg/kafka/mocks"
	. "github.com/splunk/kafka-mq-go/pkg/testing/matchers"
	. "github.com/splunk/kafka-mq-go/queue"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("Queue interface with kafka implementation", func() {
	var (
		err                error
		q                  Queue
		kafkaClientFactory *mocks.ClientFactory
		kafkaProducer      *mocks.Producer
		kafkaConsumer      *mocks.Consumer
	)

	BeforeEach(func() {
		err = nil
		kafkaClientFactory = &mocks.ClientFactory{}
		kafkaProducer = &mocks.Producer{}
		kafkaConsumer = &mocks.Consumer{}

		q = NewQueue(queueID, validOpts, kafkaClientFactory)
		Expect(q).ShouldNot(BeNil())
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
		kafkaProducer.AssertExpectations(GinkgoT())
		kafkaConsumer.AssertExpectations(GinkgoT())
	})

	Describe("CreateProducer", func() {
		var (
			p Producer
		)

		JustBeforeEach(func() {
			p, err = q.Producer()
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				pCfg := confluentKafka.ConfigMap{
					"bootstrap.servers": validOpts.BootstrapServers,
					"partitioner":       "random",
				}
				kafkaClientFactory.On("NewProducer", pCfg).Return(kafkaProducer, nil)
			})

			It("should return a producer", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(p).ShouldNot(BeNil())
			})

		})

		When("Unable to create Kafka client", func() {
			criticalError := errors.New("oops")
			BeforeEach(func() {
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(p).Should(BeNil())
			})
		})
	})

	Describe("CreateConsumer", func() {
		var (
			c            Consumer
			kProducerCfg = confluentKafka.ConfigMap{
				"bootstrap.servers": validOpts.BootstrapServers,
				"acks":              "all",
				"retries":           0,
				"linger.ms":         1,
				"partitioner":       "murmur2_random",
			}
			kConsumerCfg = confluentKafka.ConfigMap{
				"bootstrap.servers":  validOpts.BootstrapServers,
				"group.id":           "kafka-mq-" + queueID,
				"auto.offset.reset":  "earliest",
				"enable.auto.commit": "false",
			}
		)

		JustBeforeEach(func() {
			c, err = q.Consumer()
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				kafkaConsumer.On("Subscribe", mock.Anything, mock.Anything).Return(nil)

				// The consumer run loop runs in the background, and it may make calls to the kafka consumer and producer while it runs
				mockConsumerLoopMayRun(kafkaConsumer, kafkaProducer)

				kafkaClientFactory.On("NewProducer", kProducerCfg).Return(kafkaProducer, nil)
				kafkaClientFactory.On("NewConsumer", kConsumerCfg).Return(kafkaConsumer, nil)
			})
			It("should return a consumer", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(c).ShouldNot(BeNil())
			})
		})

		When("Unable to create Kafka producer client", func() {
			criticalError := errors.New("oops")
			BeforeEach(func() {
				kafkaClientFactory.On("NewConsumer", kConsumerCfg).Return(kafkaConsumer, nil)
				kafkaClientFactory.On("NewProducer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(c).Should(BeNil())
			})
		})

		When("Unable to create Kafka consumer client", func() {
			criticalError := errors.New("oops")
			BeforeEach(func() {
				kafkaClientFactory.On("NewConsumer", mock.Anything).Return(nil, criticalError)
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(c).Should(BeNil())
			})
		})
	})

})
