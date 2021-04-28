package queue_test

import (
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka/mocks"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("Producer", func() {

	var (
		p                  Producer
		err                error
		kafkaClientFactory *mocks.ClientFactory
		kafkaProducer      *mocks.Producer

		payload = []byte("test message")
	)

	BeforeEach(func() {
		kafkaClientFactory = &mocks.ClientFactory{}
		kafkaProducer = &mocks.Producer{}

		q := NewQueue(queueID, validOpts, kafkaClientFactory)
		Expect(q).ShouldNot(BeNil())

		kafkaClientFactory.On("NewProducer", mock.Anything).Return(kafkaProducer, nil)
		p, err = q.Producer()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(p).ShouldNot(BeNil())
	})

	AfterEach(func() {
		kafkaClientFactory.AssertExpectations(GinkgoT())
		kafkaProducer.AssertExpectations(GinkgoT())
	})

	Describe("Send", func() {
		JustBeforeEach(func() {
			err = p.Send(payload)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				mockProduceOK(kafkaProducer)
			})
			It("should work", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Context("Failure to produce", func() {
			BeforeEach(func() {
				mockProduceFail(kafkaProducer)
			})
			It("should return an error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})

		Context("Failure to deliver", func() {
			BeforeEach(func() {
				mockProduceDeliveryFail(kafkaProducer)
			})
			It("should return an error", func() {
				Expect(err).Should(HaveOccurred())
			})
		})
	})

	Describe("Close", func() {
		BeforeEach(func() {
			kafkaProducer.On("Close")
		})

		It("should close the kafka producer", func() {
			Expect(p.Close()).To(Succeed())
		})
	})

})
