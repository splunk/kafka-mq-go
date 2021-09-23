package functional

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/queue"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kafka-based MQ", func() {
	var (
		err           error
		service       queue.Service
		clientFactory = kafka.NewClientFactory()
	)
	BeforeEach(func() {
		err = nil
		service, err = queue.NewService(clientFactory, conf)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(service).ShouldNot(BeNil())
	})
	Describe("Single Queue", func() {
		var (
			q        queue.Queue
			p        queue.Producer
			c        queue.Consumer
			qID      = "A"
			payload1 = []byte("apple")
			m        queue.Message
		)
		BeforeEach(func() {
			q = createQueue(service, qID)
			p, c = createProducerConsumerPair(q)
		})
		AfterEach(func() {
			closeAll(p, c)
		})
		When("consumer context completes before receipt", func() {
			JustBeforeEach(func() {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				m, err = c.Receive(ctx)
			})
			It("should return without a message", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).Should(BeNil())
			})
		})
		Context("one producer, one consumer", func() {
			JustBeforeEach(func() {
				err = p.Send(payload1)
				Expect(err).ShouldNot(HaveOccurred())

				ctx := context.Background()
				m, err = c.Receive(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m).ShouldNot(BeNil())
				Expect(m.Payload()).Should(Equal(payload1))
			})
			When("consumer receives and ACKs", func() {
				It("should succeed", func() {
					err = m.Ack()
					Expect(err).ShouldNot(HaveOccurred())

					// Message should not be redelivered after ACK
					expectMessageNotRedelivered(c)
				})
			})
			When("consumer receives and NACKs", func() {
				It("should not be redelivered", func() {
					err = m.Nack()
					Expect(err).ShouldNot(HaveOccurred())

					// Message should not be redelivered after NACK
					expectMessageNotRedelivered(c)
				})
			})
			When("consumer receives and Interrupts", func() {
				It("should be redelivered", func() {
					err = m.Interrupt()
					Expect(err).ShouldNot(HaveOccurred())

					// Message should be redelivered after Interrupt
					ctx, cancel := context.WithTimeout(context.Background(), 2*conf.RedeliveryTimeout)
					defer cancel()
					m, err = c.Receive(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(m).ShouldNot(BeNil())
					Expect(m.Payload()).Should(Equal(payload1))

					err = m.Ack()
					Expect(err).ShouldNot(HaveOccurred())

					// Message should not be redelivered after ACK
					expectMessageNotRedelivered(c)
				})
			})
		})
		Context("one producer, multiple consumers", func() {
			const (
				numMessges = 500
			)
			var (
				c2 queue.Consumer
				wg sync.WaitGroup

				consumerOneReceived uint32
				consumerTwoReceived uint32
			)
			BeforeEach(func() {
				c2, err = q.Consumer()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(c).ShouldNot(BeNil())
			})
			AfterEach(func() {
				// We wait for all consumer go-routines to complete here before calling close
				// to prevent the running go-routines from calling Receive() on a closed consumer
				wg.Wait()

				closeAll(c2)
			})
			JustBeforeEach(func() {
				for i := 0; i < numMessges; i++ {
					err = p.Send(payload1)
					Expect(err).ShouldNot(HaveOccurred())
				}

				consume := func(consumer queue.Consumer) bool {
					ctx, cancel := context.WithTimeout(context.Background(), conf.RedeliveryTimeout)
					defer cancel()

					m, err := consumer.Receive(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					if m == nil {
						return false
					}
					Expect(m.Payload()).Should(Equal(payload1))
					m.Ack()
					return true
				}

				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for consume(c) {
						atomic.AddUint32(&consumerOneReceived, 1)
					}
				}()

				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for consume(c2) {
						atomic.AddUint32(&consumerTwoReceived, 1)

					}
				}()
			})
			When("consumers receive and ACK", func() {
				It("should succeed", func() {
					expectMsg := fmt.Sprintf("consumerOneReceived + consumerTwoReceived != numMessages: %d + %d != %d", consumerOneReceived, consumerTwoReceived, numMessges)
					Eventually(func() bool {
						return (consumerOneReceived + consumerTwoReceived) == numMessges
					}, 30*time.Second, 1*time.Second).Should(BeTrue(), expectMsg)
					Expect(consumerOneReceived).Should(BeNumerically(">", 0))
					Expect(consumerTwoReceived).Should(BeNumerically(">", 0))
				})
			})
		})
	})
	Describe("Multiple queues", func() {
		const (
			queueB = "B"
			queueC = "C"
		)
		var (
			// Queue B has a single producer and consumer
			qB       queue.Queue
			pB       queue.Producer
			cB       queue.Consumer
			payloadB = []byte("payload for queue B")
			mB       queue.Message
			errB     error

			// Queue B has a single producer and consumer
			qC       queue.Queue
			pC       queue.Producer
			cC       queue.Consumer
			payloadC = []byte("a different payload for queue C")
			mC       queue.Message
			errC     error
		)
		BeforeEach(func() {
			qB = createQueue(service, queueB)
			pB, cB = createProducerConsumerPair(qB)

			qC = createQueue(service, queueC)
			pC, cC = createProducerConsumerPair(qC)
		})
		AfterEach(func() {
			closeAll(pB, cB, pC, cC)
		})
		Context("two producers, two consumers", func() {
			JustBeforeEach(func() {
				errB = pB.Send(payloadB)
				Expect(errB).ShouldNot(HaveOccurred())

				errC = pC.Send(payloadC)
				Expect(errC).ShouldNot(HaveOccurred())

				mB, errB = cB.Receive(context.Background())
				Expect(errB).ShouldNot(HaveOccurred())
				Expect(mB).ShouldNot(BeNil())
				Expect(mB.Payload()).Should(Equal(payloadB))

				mC, errC = cC.Receive(context.Background())
				Expect(errC).ShouldNot(HaveOccurred())
				Expect(mC).ShouldNot(BeNil())
				Expect(mC.Payload()).Should(Equal(payloadC))
			})
			When("both consumers ACK message", func() {
				It("should succeed", func() {
					errB = mB.Ack()
					Expect(errB).ShouldNot(HaveOccurred())

					errC = mC.Ack()
					Expect(errC).ShouldNot(HaveOccurred())

					// Messages should not be redelivered after ACK
					expectMessageNotRedelivered(cB)
					expectMessageNotRedelivered(cC)
				})
			})
			When("one consumer ACKs and the other NACKs message", func() {
				It("should not be redelivered", func() {
					errB = mB.Ack()
					Expect(errB).ShouldNot(HaveOccurred())

					errC = mC.Nack()
					Expect(errC).ShouldNot(HaveOccurred())

					// Messages should not be redelivered after ACK or NACK
					expectMessageNotRedelivered(cB)
					expectMessageNotRedelivered(cC)
				})
			})
			When("consumer ACKs and the other Interrupts message", func() {
				It("should be redelivered", func() {
					errB = mB.Interrupt()
					Expect(errB).ShouldNot(HaveOccurred())

					errC = mC.Ack()
					Expect(errC).ShouldNot(HaveOccurred())

					// Message should be redelivered after Interrupt
					ctx, cancel := context.WithTimeout(context.Background(), 2*conf.RedeliveryTimeout)
					defer cancel()
					mB, errB = cB.Receive(ctx)
					Expect(errB).ShouldNot(HaveOccurred())
					Expect(mB).ShouldNot(BeNil())
					Expect(mB.Payload()).Should(Equal(payloadB))

					errB = mB.Ack()
					Expect(errB).ShouldNot(HaveOccurred())

					// Messages should not be redelivered after ACK
					expectMessageNotRedelivered(cB)
					expectMessageNotRedelivered(cC)
				})
			})
		})
	})
})

func createQueue(s queue.Service, id string) queue.Queue {
	q := s.GetOrCreateQueue(id)
	Expect(q).ShouldNot(BeNil())

	return q
}

func createProducerConsumerPair(q queue.Queue) (queue.Producer, queue.Consumer) {
	p, err := q.Producer()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(p).ShouldNot(BeNil())

	c, err := q.Consumer()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(c).ShouldNot(BeNil())

	return p, c
}

func closeAll(closers ...io.Closer) {
	for _, c := range closers {
		err := c.Close()
		Expect(err).ShouldNot(HaveOccurred())
	}
}

func expectMessageNotRedelivered(c queue.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*conf.RedeliveryTimeout)
	defer cancel()
	m, err := c.Receive(ctx)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(m).Should(BeNil())
}
