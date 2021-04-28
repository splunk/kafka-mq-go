package queue_test

import (
	"errors"
	"time"

	. "cd.splunkdev.com/dferstay/kafka-mq-go/pkg/testing/matchers"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal/mocks"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("Message", func() {
	var (
		err      error
		m        *KafkaMessage
		mqClient *mocks.MqClient

		testValue       = []byte("test-value")
		keepAlivePeriod = 100 * time.Millisecond
		kMsg            = &confluentKafka.Message{
			Value: testValue,
		}

		criticalError = errors.New("oops")
	)

	BeforeEach(func() {
		err = nil
		mqClient = &mocks.MqClient{}

		m, err = NewMessage(queueID, mqClient, keepAlivePeriod, kMsg)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(m).ShouldNot(BeNil())
	})

	AfterEach(func() {
		mqClient.AssertExpectations(GinkgoT())
	})

	Describe("NewMessage", func() {
		JustBeforeEach(func() {
			m, err = NewMessage(queueID, mqClient, keepAlivePeriod, nil)
		})
		When("called with a nil kafka message", func() {
			It("should return an error", func() {
				Expect(err).Should(HaveOccurred())
				Expect(m).Should(BeNil())
			})
		})
	})

	Describe("Payload", func() {
		var (
			payload []byte
		)

		JustBeforeEach(func() {
			payload = m.Payload()
		})

		Context("Happy path", func() {
			It("should return the message bytes", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(payload).ShouldNot(BeNil())
				Expect(payload).To(Equal(testValue))
			})
		})

	})

	Describe("Ack", func() {
		JustBeforeEach(func() {
			err = m.Ack()
			m.StartKeepAliveTimer()
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				mqClient.On("Processed", kMsg).Return(nil)

				m, err = NewMessage(queueID, mqClient, keepAlivePeriod, kMsg)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
			})

			It("should mark the message as processed", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("Failure to mark the message processed", func() {
			BeforeEach(func() {
				mqClient.On("Processed", kMsg).Return(criticalError)

				m, err = NewMessage(queueID, mqClient, keepAlivePeriod, kMsg)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})
	})

	Describe("Nack", func() {
		JustBeforeEach(func() {
			err = m.Nack()
		})

		Context("Happy path", func() {
			BeforeEach(func() {
				mqClient.On("Processed", kMsg).Return(nil)

				m, err = NewMessage(queueID, mqClient, keepAlivePeriod, kMsg)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
			})

			It("should mark the message as processed", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("Failure to mark the message processed", func() {
			BeforeEach(func() {
				mqClient.On("Processed", kMsg).Return(criticalError)

				m, err = NewMessage(queueID, mqClient, keepAlivePeriod, kMsg)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
			})

			It("should propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
			})
		})
	})

	Describe("Interrupt", func() {
		JustBeforeEach(func() {
			m.StartKeepAliveTimer()
			err = m.Interrupt()
		})

		Context("interrupt process", func() {
			BeforeEach(func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
			})

			It("should stop keep-alive timer", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Consistently(func() bool {
					return mqClient.AssertNotCalled(GinkgoT(), "ProcessingInProgress", mock.Anything)
				}, keepAlivePeriod*2).Should(BeTrue())
			})
		})
	})

	Describe("Keep-alive", func() {
		JustBeforeEach(func() {
			m.StartKeepAliveTimer()
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				mqClient.On("ProcessingInProgress", kMsg).Return(nil)
			})
			It("should eventually call mqclient processing in progress", func() {
				Eventually(func() bool {
					return methodWasCalled(&mqClient.Mock, "ProcessingInProgress", kMsg)
				}).Should(BeTrue())
			})
		})
	})

})
