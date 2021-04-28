package redelivery_test

import (
	"time"

	kafkamq "cd.splunkdev.com/dferstay/kafka-mq-go/queue/proto"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/redelivery"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MarkersQueue", func() {
	var (
		q              redelivery.MarkersQueue
		ok             bool
		toRedeliver    []*kafkamq.Marker
		smallestOffset int64

		zeroTime   = time.Time{}
		zeroOffset = int64(0)
		start1     = newStartMarker(0, 1000)
		start2     = newStartMarker(1, 1000)
		start3     = newStartMarker(2, 1000)
	)

	BeforeEach(func() {
		q = redelivery.NewMarkersQueue(0)
		Expect(q).ShouldNot(BeNil())

		ok = true
		toRedeliver = []*kafkamq.Marker{}
		smallestOffset = 0
	})

	JustBeforeEach(func() {
		smallestOffset, ok = q.SmallestMarkerOffset()
	})

	Context("Start markers", func() {
		When("No markers have been added", func() {
			It("should not indicate that there are markers to redeliver", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime)
				Expect(toRedeliver).Should(BeEmpty())

				Expect(ok).Should(BeFalse())
				Expect(smallestOffset).Should(Equal(zeroOffset))
			})
		})
		When("one start marker is added", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
			})
			It("should not redeliver if the deadline has not passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime)
				Expect(toRedeliver).Should(BeEmpty())

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
			It("should redeliver if the deadline has passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(1))
				Expect(toRedeliver[0]).Should(Equal(start1))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
		When("multiple start markers have been added", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
				q.AddMarker(1, zeroTime.Add(1*time.Second), start2)
				q.AddMarker(2, zeroTime.Add(2*time.Second), start3)
			})
			It("should not redeliver any if no deadlines have passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime)
				Expect(toRedeliver).Should(BeEmpty())

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
			It("should redeliver one if a single deadline has passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(1))
				Expect(toRedeliver[0]).Should(Equal(start1))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
			It("should redeliver all if all deadlines have passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(3))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start2))
				Expect(toRedeliver[2]).Should(Equal(start3))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
		When("duplicate start markers have been added", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)

				start1_dup := *start1
				q.AddMarker(1, zeroTime.Add(1*time.Second), &start1_dup)
			})
			It("should udpate the redelivery deadline and leave the smallest markers offset unchanged", func() {
				// the initial start marker had a deadline of 1 second, check that the deadline has been updated
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(BeEmpty())

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(2001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(1))
				Expect(toRedeliver[0]).Should(Equal(start1))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
		When("start markers are added out of order", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
				q.AddMarker(2, zeroTime.Add(2*time.Second), start3)
				q.AddMarker(1, zeroTime.Add(1*time.Second), start2)
			})
			It("should not redeliver any if no deadlines have passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime)
				Expect(toRedeliver).Should(BeEmpty())

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
			It("should redeliver one if a single deadline has passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(1))
				Expect(toRedeliver[0]).Should(Equal(start1))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
			It("should redeliver all if all deadlines have passed", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(3))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start2))
				Expect(toRedeliver[2]).Should(Equal(start3))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
	})

	Context("Keep-alive markers", func() {
		BeforeEach(func() {
			q.AddMarker(0, zeroTime, start1)
			q.AddMarker(1, zeroTime.Add(1*time.Second), start2)
			q.AddMarker(2, zeroTime.Add(2*time.Second), start3)
		})
		When("a keep-alive marker is added for a marker that is not in progress", func() {
			It("should not update any redelivery deadlines", func() {
				// keep-alive refers to a message that is not in progress (by offset)
				ka := newKeepAliveMarker(99, 1000)
				q.AddMarker(4, zeroTime.Add(3*time.Second), ka)

				// should have no effect on list of messages require redelivery
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(3))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start2))
				Expect(toRedeliver[2]).Should(Equal(start3))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
		When("a keep-alive marker is added for a marker that is in progress", func() {
			It("should update the redelivery deadline of the marker", func() {
				// the initial start marker had a deadline of 1 second, check that the deadline has been updated
				ka := newKeepAliveMarker(start1.MessageId.Offset, 1000)
				q.AddMarker(4, zeroTime.Add(3*time.Second), ka)

				// nothing redelivered after 1 second
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(BeEmpty())

				// marker 2 redelivered after 23 seconds
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(2001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(1))
				Expect(toRedeliver[0]).Should(Equal(start2))

				// marker 1 still has the smallest offset
				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
	})
	When("Marker Queue is empty", func() {

		When("Receive keep-alive marker", func() {
			It("should not update any redelivery deadlines", func() {
				ka := newKeepAliveMarker(99, 1000)
				q.AddMarker(1, zeroTime.Add(1*time.Second), ka)

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(BeEmpty())
				Expect(ok).Should(BeFalse())
			})
		})
		When("Receive end marker", func() {
			It("should not update any redelivery deadlines", func() {
				e := newEndMarker(start1.MessageId.Offset)
				q.AddMarker(4, zeroTime.Add(1*time.Second), e)

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(BeEmpty())
				Expect(ok).Should(BeFalse())
			})
		})
	})

	Context("End markers", func() {
		When("an end marker is added for a marker that is not in progress", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
				q.AddMarker(1, zeroTime.Add(1*time.Second), start2)
				q.AddMarker(2, zeroTime.Add(2*time.Second), start3)
			})
			It("should not update any redelivery deadlines", func() {
				// end refers to a message that is not in progress (by offset)
				e := newEndMarker(99)
				q.AddMarker(4, zeroTime.Add(3*time.Second), e)

				// should have no effect on list of messages require redelivery
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(3))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start2))
				Expect(toRedeliver[2]).Should(Equal(start3))

				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))
			})
		})
		When("a single start marker is in progress", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
			})
			JustBeforeEach(func() {
				// end should terminate processing of start marker 1
				e := newEndMarker(start1.MessageId.Offset)
				q.AddMarker(4, zeroTime.Add(3*time.Second), e)

				smallestOffset, ok = q.SmallestMarkerOffset()
			})
			It("a single end marker should remove the message from the redelivery list and zero-out the smallest offset", func() {
				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(1001 * time.Millisecond))
				Expect(toRedeliver).Should(BeEmpty())

				Expect(ok).Should(BeFalse())
				Expect(smallestOffset).Should(BeEquivalentTo(0))
			})
		})
		When("multiple markers are in progress", func() {
			BeforeEach(func() {
				q.AddMarker(0, zeroTime, start1)
				q.AddMarker(1, zeroTime.Add(1*time.Second), start2)
				q.AddMarker(2, zeroTime.Add(2*time.Second), start3)
			})
			It("a single end marker should remove the earliest marker from the redelivery list", func() {
				e := newEndMarker(start1.MessageId.Offset)
				q.AddMarker(4, zeroTime.Add(3*time.Second), e)

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(2))
				Expect(toRedeliver[0]).Should(Equal(start2))
				Expect(toRedeliver[1]).Should(Equal(start3))

				smallestOffset, ok = q.SmallestMarkerOffset()
				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start2.MessageId.Offset))

			})
			It("a single end marker should remove the latest marker from the redelivery list", func() {
				e := newEndMarker(start3.MessageId.Offset)
				q.AddMarker(4, zeroTime.Add(3*time.Second), e)

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(2))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start2))

				smallestOffset, ok = q.SmallestMarkerOffset()
				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))

			})
			It("a single end marker should remove a marker from the milddle of the redelivery list", func() {
				e := newEndMarker(start2.MessageId.Offset)
				q.AddMarker(4, zeroTime.Add(3*time.Second), e)

				toRedeliver = q.MarkersToRedeliver(zeroTime.Add(3001 * time.Millisecond))
				Expect(toRedeliver).Should(HaveLen(2))
				Expect(toRedeliver[0]).Should(Equal(start1))
				Expect(toRedeliver[1]).Should(Equal(start3))

				smallestOffset, ok = q.SmallestMarkerOffset()
				Expect(ok).Should(BeTrue())
				Expect(smallestOffset).Should(Equal(start1.MessageId.Offset))

			})
		})
	})
})
