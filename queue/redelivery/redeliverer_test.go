package redelivery_test

import (
	"errors"
	"fmt"

	"github.com/splunk/kafka-mq-go/queue/internal/mocks"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"
	. "github.com/splunk/kafka-mq-go/queue/redelivery"

	"github.com/cenkalti/backoff"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Redeliverer", func() {
	const (
		partition  = 1
		numRetries = 16
	)
	var (
		r                  Redeliverer
		messageSender      *mocks.MessageSender
		markerProducer     *mocks.MarkerProducer
		exponentialBackoff backoff.BackOff
	)
	BeforeEach(func() {
		r = nil
		messageSender = &mocks.MessageSender{}
		markerProducer = &mocks.MarkerProducer{}
		exponentialBackoff = backoff.NewExponentialBackOff()
	})
	AfterEach(func() {
		messageSender.AssertExpectations(GinkgoT())
		markerProducer.AssertExpectations(GinkgoT())
	})
	Describe("NewRedeliverer", func() {
		JustBeforeEach(func() {
			r = NewRedeliverer(partition, messageSender, markerProducer, exponentialBackoff)
		})
		Context("Happy path", func() {
			It("should return a Redeliverer", func() {
				Expect(r).ShouldNot(BeNil())
			})
		})
	})
	Describe("Redeliver", func() {
		var (
			markers       []*kafkamq.Marker
			queueID       = "test-queue"
			criticalError = errors.New("oops")
			expectedMsg   *confluentKafka.Message
		)
		BeforeEach(func() {
			r = NewRedeliverer(partition, messageSender, markerProducer, exponentialBackoff)
			Expect(r).ShouldNot(BeNil())
		})
		AfterEach(func() {
			expectedMsg = nil
		})
		JustBeforeEach(func() {
			r.Redeliver(markers)
		})
		When("the redelivery list is empty", func() {
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 0)
			})
			It("should not attempt any redeliveries", func() {
				// assertions are made on the calls to message and marker producers
			})
		})
		When("the redelivery list contains a single end marker", func() {
			BeforeEach(func() {
				markers = []*kafkamq.Marker{
					&kafkamq.Marker{
						Type: kafkamq.Marker_END,
						MessageId: &kafkamq.MessageID{
							Partition: int32(partition),
							Offset:    int64(0),
						},
					},
				}
			})
			It("should not attempt any redeliveries", func() {
				// assertions are made on the calls to message and marker producers
			})
		})
		When("the redelivery list contains a single keep-alive marker", func() {
			BeforeEach(func() {
				markers = []*kafkamq.Marker{
					&kafkamq.Marker{
						Type: kafkamq.Marker_KEEPALIVE,
						MessageId: &kafkamq.MessageID{
							Partition: int32(partition),
							Offset:    int64(0),
						},
					},
				}
			})
			It("should not attempt any redeliveries", func() {
				// assertions are made on the calls to message and marker producers
			})
		})
		When("the redelivery list contains a single start marker", func() {
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 1)
				expectedMsg = createMessage(partition, 0, []byte(queueID), []byte(fmt.Sprint(0)))

				messageSender.On("SendMessage", queueID, expectedMsg.Value).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg).Return(nil).Once()
			})
			It("should redeliver the message", func() {
				// assertions are made on the calls to message and marker producers
				Eventually(func() bool {
					return methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg.Value) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg)
				}).Should(BeTrue())
			})
		})
		When("the redelivery list contains a more than one start marker", func() {
			var (
				expectedMsg1 *confluentKafka.Message
				expectedMsg2 *confluentKafka.Message
				expectedMsg3 *confluentKafka.Message
			)
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 3)
				expectedMsg1 = createMessage(partition, 0, []byte(queueID), []byte(fmt.Sprint(0)))
				expectedMsg2 = createMessage(partition, 1, []byte(queueID), []byte(fmt.Sprint(1)))
				expectedMsg3 = createMessage(partition, 2, []byte(queueID), []byte(fmt.Sprint(2)))

				messageSender.On("SendMessage", queueID, expectedMsg1.Value).Return(nil).Once()
				messageSender.On("SendMessage", queueID, expectedMsg2.Value).Return(nil).Once()
				messageSender.On("SendMessage", queueID, expectedMsg3.Value).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg1).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg2).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg3).Return(nil).Once()
			})
			It("should redeliver the messages", func() {
				// assertions are made on the calls to message and marker producers
				Eventually(func() bool {
					return methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg1.Value) &&
						methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg2.Value) &&
						methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg3.Value) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg1) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg2) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg3)
				}).Should(BeTrue())
			})
		})
		When("the message cannot be redelivered", func() {
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 1)
				expectedMsg = createMessage(partition, 0, []byte(queueID), []byte(fmt.Sprint(0)))

				messageSender.On("SendMessage", queueID, expectedMsg.Value).Return(criticalError)
			})
			It("should not redeliver the message", func() {
				// assertions are made on the calls to message and marker producers
				Eventually(func() bool {
					return methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg.Value)
				}).Should(BeTrue())
			})
		})
		When("the message can be redelivered but the marker cannot", func() {
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 1)
				expectedMsg = createMessage(partition, 0, []byte(queueID), []byte(fmt.Sprint(0)))

				messageSender.On("SendMessage", queueID, expectedMsg.Value).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg).Return(criticalError)
			})
			It("should redeliver the message", func() {
				Eventually(func() bool {
					return methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg.Value) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg)
				}).Should(BeTrue())
				r.Stop()
			})
		})
		When("the message cannot be redelivered it will be retried", func() {
			BeforeEach(func() {
				markers = createRedeliveryList(queueID, partition, 1)
				expectedMsg = createMessage(partition, 0, []byte(queueID), []byte(fmt.Sprint(0)))

				messageSender.On("SendMessage", queueID, expectedMsg.Value).Return(criticalError).Once()
				messageSender.On("SendMessage", queueID, expectedMsg.Value).Return(nil).Once()
				markerProducer.On("SendEndMarker", queueID, expectedMsg).Return(nil).Once()
			})
			It("should redeliver the message", func() {
				// assertions are made on the calls to message and marker producers
				Eventually(func() bool {
					return methodWasCalled(&messageSender.Mock, "SendMessage", queueID, expectedMsg.Value) &&
						methodWasCalled(&markerProducer.Mock, "SendEndMarker", queueID, expectedMsg)
				}).Should(BeTrue())
			})
		})
	})
})

func createRedeliveryList(queueID string, partition int, n int) []*kafkamq.Marker {
	var ret = []*kafkamq.Marker{}
	for i := 0; i < n; i++ {
		ret = append(ret, &kafkamq.Marker{
			Type:  kafkamq.Marker_START,
			Value: []byte(fmt.Sprint(i)),
			Key:   []byte(queueID),
			MessageId: &kafkamq.MessageID{
				Partition: int32(partition),
				Offset:    int64(i),
			},
			RedeliverAfterMs: 600,
		})
	}
	return ret
}
