package redelivery_test

import (
	"time"

	"github.com/splunk/kafka-mq-go/queue/redelivery"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Redelivery Priority Queues", func() {
	var (
		ok bool
	)

	BeforeEach(func() {
		ok = true
	})

	Describe("MarkerOffsetsPQ", func() {
		var (
			pq     *redelivery.MarkerOffsetsPQ
			item   redelivery.MarkerOffset
			first  = newMarkerOffset(0, 0, 0)
			second = newMarkerOffset(0, 1, 1)
			third  = newMarkerOffset(0, 2, 2)
		)
		BeforeEach(func() {
			pq = redelivery.NewMarkerOffsetsPQ()
			item = redelivery.MarkerOffset{}
		})
		Context("Head", func() {
			When("Empty", func() {
				It("should indicate that the head is empty", func() {
					item, ok = pq.Head()
					Expect(ok).Should(BeFalse())
					Expect(item).Should(Equal(redelivery.MarkerOffset{}))
				})
			})
			When("Non-empty", func() {
				JustBeforeEach(func() {
					// enqueue the items out of order in order to force sorting by minimum offset
					ok = pq.Enqueue(second)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(third)
					Expect(ok).Should(BeTrue())
				})
				It("should return the item at the head of the queue", func() {
					item, ok = pq.Head()
					Expect(ok).Should(BeTrue())
					Expect(item).Should(Equal(first))
				})
			})
		})
		Context("Dequeue", func() {
			When("Empty", func() {
				It("should indicate that the queue is empty", func() {
					item, ok = pq.Dequeue()
					Expect(ok).Should(BeFalse())
					Expect(item).Should(Equal(redelivery.MarkerOffset{}))
				})
			})
			When("Non-empty", func() {
				JustBeforeEach(func() {
					// enqueue the items out of order in order to force sorting by minimum offset
					ok = pq.Enqueue(second)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(third)
					Expect(ok).Should(BeTrue())
				})
				It("should dequeue the elements in order", func() {
					expected := []redelivery.MarkerOffset{
						first, second, third,
					}
					for i := 0; i < len(expected); i++ {
						e := expected[i]
						item, ok := pq.Dequeue()
						Expect(ok).Should(BeTrue())
						Expect(item).Should(Equal(e))
					}
				})
			})
		})
		Context("Enqueue", func() {
			When("Not enqueing a duplicate marker", func() {
				It("should succeed", func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())
				})
			})
			When("Enqueuing a duplicate marker", func() {
				JustBeforeEach(func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())
				})
				It("should fail", func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeFalse())
				})
			})
		})
	})

	Describe("MarkerDeadlinesPQ", func() {
		var (
			pq       *redelivery.MarkerDeadlinesPQ
			item     redelivery.MarkerDeadline
			zeroTime = time.Time{}
			first    = newMarkerDeadline(0, 0, zeroTime)
			second   = newMarkerDeadline(0, 2, zeroTime.Add(1*time.Second))
			third    = newMarkerDeadline(0, 3, zeroTime.Add(2*time.Second))
		)

		BeforeEach(func() {
			pq = redelivery.NewMarkerDeadlinesPQ()
			item = redelivery.MarkerDeadline{}
		})

		Context("Head", func() {
			When("Empty", func() {
				It("should indicate that the head is empty", func() {
					item, ok = pq.Head()
					Expect(ok).Should(BeFalse())
					Expect(item).Should(Equal(redelivery.MarkerDeadline{}))
				})
			})
			When("Non-empty", func() {
				JustBeforeEach(func() {
					// enqueue the items out of order in order to force sorting by minimum offset
					ok = pq.Enqueue(second)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(third)
					Expect(ok).Should(BeTrue())
				})
				It("should return the item at the head of the queue", func() {
					item, ok = pq.Head()
					Expect(ok).Should(BeTrue())
					Expect(item).Should(Equal(first))
				})
			})
		})
		Context("Dequeue", func() {
			When("Empty", func() {
				It("should indicate that the queue is empty", func() {
					item, ok = pq.Dequeue()
					Expect(ok).Should(BeFalse())
					Expect(item).Should(Equal(redelivery.MarkerDeadline{}))
				})
			})
			When("Non-empty", func() {
				JustBeforeEach(func() {
					// enqueue the items out of order in order to force sorting by minimum offset
					ok = pq.Enqueue(second)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(third)
					Expect(ok).Should(BeTrue())
				})
				It("should dequeue the elements in order", func() {
					expected := []redelivery.MarkerDeadline{
						first, second, third,
					}
					for i := 0; i < len(expected); i++ {
						e := expected[i]
						item, ok := pq.Dequeue()
						Expect(ok).Should(BeTrue())

						// We don't compare for struct equality here because of the non-exported `index` field
						// which is set to -1 by the heap implementation on dequeue
						Expect(item.MessageID).Should(Equal(e.MessageID))
						Expect(item.RedeliveryDeadline).Should(Equal(e.RedeliveryDeadline))
					}
				})
			})
		})
		Context("Enqueue", func() {
			When("Not enqueing a duplicate marker", func() {
				It("should succeed", func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())
				})
			})
			When("Enqueuing a duplicate marker", func() {
				JustBeforeEach(func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())
				})
				It("should fail", func() {
					ok = pq.Enqueue(first)
					Expect(ok).Should(BeFalse())
				})
			})
		})
		Context("Update", func() {
			When("Referencing a message that is not in the queue", func() {
				JustBeforeEach(func() {
					ok = pq.Update(first.MessageID, zeroTime)
				})
				It("should fail", func() {
					Expect(ok).Should(BeFalse())
				})
			})
			When("Referencing a message that is in the queue", func() {
				var updatedRedeliverAfter = first.RedeliveryDeadline.Add(3 * time.Second)
				JustBeforeEach(func() {
					// enqueue the items out of order in order to force sorting by minimum offset
					ok = pq.Enqueue(second)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(first)
					Expect(ok).Should(BeTrue())

					ok = pq.Enqueue(third)
					Expect(ok).Should(BeTrue())

					ok = pq.Update(first.MessageID, updatedRedeliverAfter)
					Expect(ok).Should(BeTrue())
				})
				It("should succeed", func() {
					firstUpdated := first
					firstUpdated.RedeliveryDeadline = updatedRedeliverAfter

					expected := []redelivery.MarkerDeadline{
						second, third, firstUpdated,
					}
					for i := 0; i < len(expected); i++ {
						e := expected[i]
						item, ok := pq.Dequeue()
						Expect(ok).Should(BeTrue())

						// We don't compare for struct equality here because of the non-exported `index` field
						// which is set to -1 by the heap implementation on dequeue
						Expect(item.MessageID).Should(Equal(e.MessageID))
						Expect(item.RedeliveryDeadline).Should(Equal(e.RedeliveryDeadline))
					}
				})
			})
		})
	})
})

func newMarkerOffset(messagePartition int, messageOffset int, markerOffset int) redelivery.MarkerOffset {
	id := redelivery.NewMessageID(int32(messagePartition), int64(messageOffset))
	return redelivery.MarkerOffset{
		MessageID:      id,
		OffsetOfMarker: int64(markerOffset),
	}
}

func newMarkerDeadline(messagePartition int, messageOffset int, redeliveryDeadline time.Time) redelivery.MarkerDeadline {
	id := redelivery.NewMessageID(int32(messagePartition), int64(messageOffset))
	return redelivery.MarkerDeadline{
		MessageID:          id,
		RedeliveryDeadline: redeliveryDeadline,
	}
}
