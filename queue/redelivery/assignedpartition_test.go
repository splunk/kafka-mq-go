package redelivery_test

import (
	"time"

	kafkamq "cd.splunkdev.com/dferstay/kafka-mq-go/queue/proto"
	. "cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery/mocks"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AssignedPartition", func() {
	const (
		partition                    = 1
		zeroSeconds                  = 0 * time.Second
		durationUseNowIfNoMarkerSeen = 1 * time.Second
	)
	var (
		ap  AssignedPartition
		r   *mocks.Redeliverer
		mq  *mocks.MarkersQueue
		err error
	)
	BeforeEach(func() {
		mq = &mocks.MarkersQueue{}
		mq.On("Partition").Return(int32(partition))

		r = &mocks.Redeliverer{}
		ap = NewAssignedPartition(mq, r, durationUseNowIfNoMarkerSeen)
		Expect(ap).ShouldNot(BeNil())
		err = nil
	})
	AfterEach(func() {
		r.AssertExpectations(GinkgoT())
		mq.AssertExpectations(GinkgoT())
	})
	Describe("NewAssignedPartition", func() {
		Context("Happy path", func() {
			It("should return an AssignedPartition", func() {
				Expect(ap).ShouldNot(BeNil())
			})
		})
	})
	Describe("AddMessage", func() {
		var (
			markerMsg *confluentKafka.Message
			now       = time.Time{}.Add(1 * time.Second)
		)
		BeforeEach(func() {
			markerMsg = nil
		})
		Context("Happy path", func() {
			JustBeforeEach(func() {
				msgOffset := int64(99)
				msgRedeliverAfterMs := uint64(60000)
				marker := newStartMarker(msgOffset, msgRedeliverAfterMs)

				markerOffset := 0
				markerTimestamp := time.Time{}
				markerMsg = createMarkerMessage(partition, markerOffset, markerTimestamp, marker)

				mq.On("AddMarker", int64(markerOffset), markerTimestamp, marker).Return()

				err = ap.AddMessage(markerMsg, now)
			})
			It("should succeed", func() {
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		When("the msg is nil", func() {
			JustBeforeEach(func() {
				err = ap.AddMessage(markerMsg, now)
			})
			It("should return an error", func() {
				Expect(err).Should(MatchError(ContainSubstring("message must not be nil")))
			})
		})
		When("the msg is for a different partition", func() {
			JustBeforeEach(func() {
				markerMsg = createMessage(partition+1, 0, []byte("key"), []byte("value"))
				err = ap.AddMessage(markerMsg, now)
			})
			It("should return an error", func() {
				Expect(err).Should(MatchError(ContainSubstring("received a message for a different partition")))
			})
		})
		When("the msg cannot be decoded as a marker", func() {
			JustBeforeEach(func() {
				markerMsg = createMessage(partition, 0, []byte("key"), []byte("this is not a marker"))
				err = ap.AddMessage(markerMsg, now)
			})
			It("should return an error", func() {
				Expect(err).Should(MatchError(ContainSubstring("Could not unmarshal marker from message")))
			})
		})
	})
	Describe("SmallestMarkerOffset", func() {
		var (
			smallestOffset int64
			ok             bool
		)
		JustBeforeEach(func() {
			smallestOffset, ok = ap.SmallestMarkerOffset()
		})
		Context("Happy path", func() {
			var (
				markerOffset = 1
			)
			BeforeEach(func() {
				mq.On("SmallestMarkerOffset").Once().Return(int64(markerOffset), true)
			})
			It("should return the smallest marker offset", func() {
				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(BeEquivalentTo(markerOffset))
			})
		})
		When("no markers have been added to the assigned partition", func() {
			BeforeEach(func() {
				mq.On("SmallestMarkerOffset").Once().Return(int64(0), false)
			})
			It("should return ok==false", func() {
				Expect(ok).Should(BeFalse())
			})
		})
	})
	Describe("Redeliver", func() {
		var (
			now             = time.Time{}.Add(6 * time.Second)
			markerOffset    int
			markerTimestamp time.Time
			markerMsg       *confluentKafka.Message
			marker          *kafkamq.Marker
		)
		BeforeEach(func() {
			msgOffset := int64(99)
			msgRedeliverAfterMs := uint64(60000)
			marker = newStartMarker(msgOffset, msgRedeliverAfterMs)

			markerOffset = 0
			markerTimestamp = time.Time{}.Add(2 * time.Second)
			markerMsg = createMarkerMessage(partition, markerOffset, markerTimestamp, marker)
		})
		When("no markers have been added to the assigned partition", func() {
			JustBeforeEach(func() {
				ap.Redeliver(now)
			})
			It("should not attempt redelivery", func() {
				// assertions on MarkersQueue & Redeliverer mocks
			})
		})
		When("a marker has been seen recently", func() {
			BeforeEach(func() {
				mq.On("AddMarker", int64(markerOffset), markerTimestamp, marker).Once().Return()

				// Add a message with the same `now` value that is used
				// when calling `ap.Redeliver()`
				err = ap.AddMessage(markerMsg, now)

				// MarkersToRedeliver will be called with the marker timestamp
				toRedeliver := []*kafkamq.Marker{
					marker,
				}
				mq.On("MarkersToRedeliver", markerTimestamp).Once().Return(toRedeliver)

				r.On("Redeliver", toRedeliver).Once().Return()
			})
			JustBeforeEach(func() {
				ap.Redeliver(now)
			})
			It("should use the timestamp of the last marker for redelivery", func() {
				// assertions on MarkersQueue & Redeliverer mocks
			})
		})
		When("a marker has not been seen recently", func() {
			BeforeEach(func() {
				mq.On("AddMarker", int64(markerOffset), markerTimestamp, marker).Once().Return()

				// Add a message at a time older than the durationUseNow policy
				timeWhenMsgAdded := now.Add(-(durationUseNowIfNoMarkerSeen + 1*time.Second))

				err = ap.AddMessage(markerMsg, timeWhenMsgAdded)

				// MarkersToRedeliver will be called with the local time
				toRedeliver := []*kafkamq.Marker{
					marker,
				}
				mq.On("MarkersToRedeliver", timeWhenMsgAdded).Once().Return(toRedeliver)

				r.On("Redeliver", toRedeliver).Once().Return()
			})
			JustBeforeEach(func() {
				ap.Redeliver(now)
			})
			It("should use local time for redelivery", func() {
				// assertions on MarkersQueue & Redeliverer mocks
			})
		})
	})
	Describe("Close", func() {
		// Close cannot fail, there is no unhappy path
		Context("Happy path", func() {
			BeforeEach(func() {
				r.On("Stop").Once().Return()
			})
			JustBeforeEach(func() {
				ap.Close()
			})
			It("should succeed", func() {
				// assertion that ap.redeliverer.Stop() has been called
			})
		})
	})
})

func createMarkerMessage(partition int, offset int, timestamp time.Time, marker *kafkamq.Marker) *confluentKafka.Message {
	data, err := marker.Marshal()
	Expect(err).ShouldNot(HaveOccurred())

	return createMessageWithTimestamp(partition, offset, timestamp, []byte("key"), data)
}
