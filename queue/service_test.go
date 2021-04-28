package queue_test

import (
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka/mocks"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Service interface with kafka implementation", func() {
	var (
		err                error
		service            Service
		q                  Queue
		kafkaClientFactory *mocks.ClientFactory
	)

	BeforeEach(func() {
		err = nil
		kafkaClientFactory = &mocks.ClientFactory{}
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
	})

	Describe("NewService", func() {
		var (
			opts Config
		)

		BeforeEach(func() {
			opts = validOpts
		})

		JustBeforeEach(func() {
			service, err = NewService(kafkaClientFactory, opts)
		})
		Context("With valid options", func() {
			It("should work", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(service).ShouldNot(BeNil())
			})
		})
		Context("With RedeliveryTimeout of zero", func() {
			BeforeEach(func() {
				opts.RedeliveryTimeout = 0
			})
			It("should fail", func() {
				Expect(err).Should(MatchError(ContainSubstring("RedeliveryTimeout must be positive")))
				Expect(service).Should(BeNil())
			})
		})
		Context("With RedeliveryTimeout greater than max", func() {
			BeforeEach(func() {
				opts.RedeliveryTimeout = (12 * time.Hour) + 1
			})
			It("should fail", func() {
				Expect(err).Should(MatchError(ContainSubstring("RedeliveryTimeout too large")))
				Expect(service).Should(BeNil())
			})
		})
		Context("With invalid RedeliveryTracker.UseNowIfNoMarkerSeen", func() {
			BeforeEach(func() {
				opts.RedeliveryTracker.UseNowIfNoMarkerSeen = -1
			})
			It("should fail", func() {
				Expect(err).Should(MatchError(ContainSubstring("UseNowIfNoMarkerSeen must be non-negative")))
				Expect(service).Should(BeNil())
			})
		})
		Context("With invalid ConsumerNextMessageTimeout", func() {
			BeforeEach(func() {
				opts.ConsumerNextMessageTimeout = 0
			})
			It("should fail", func() {
				Expect(err).Should(MatchError(ContainSubstring("ConsumerNextMessageTimeout must be positive")))
				Expect(service).Should(BeNil())
			})
		})
		Context("With invalid ConsumerMaxMessagesPerCommit", func() {
			BeforeEach(func() {
				opts.ConsumerMaxMessagesPerCommit = 0
			})
			It("should fail", func() {
				Expect(err).Should(MatchError(ContainSubstring("ConsumerMaxMessagesPerCommit must be positive")))
				Expect(service).Should(BeNil())
			})
		})
	})

	Context("Service is created", func() {
		BeforeEach(func() {
			service, err = NewService(kafkaClientFactory, validOpts)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(service).ShouldNot(BeNil())
		})

		Describe("GetOrCreateQueue", func() {
			JustBeforeEach(func() {
				q = service.GetOrCreateQueue(queueID)
			})
			Context("Happy path", func() {

				It("should return a Queue", func() {
					Expect(q).ShouldNot(BeNil())
				})
			})
		})
	})

})
