package redelivery_test

import (
	"errors"
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka/mocks"
	. "github.com/splunk/kafka-mq-go/pkg/testing/matchers"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"
	. "github.com/splunk/kafka-mq-go/queue/redelivery"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
)

var _ = Describe("MarkerConsumer", func() {
	var (
		clients            *mocks.ClientFactory
		kc                 *mocks.Consumer
		kpMessageSender    *mocks.Producer
		kpMarkerProducer   *mocks.Producer
		mc                 MarkerConsumer
		err                error
		kpMessageSenderCfg = confluentKafka.ConfigMap{
			"bootstrap.servers": validOpts.BootstrapServers,
			"partitioner":       "random",
		}
		kpMarkerProducerCfg = confluentKafka.ConfigMap{
			"bootstrap.servers": validOpts.BootstrapServers,
			"acks":              "all",
			"retries":           0,
			"linger.ms":         1,
			"partitioner":       "murmur2_random",
		}
		criticalError = errors.New("oops")
	)

	BeforeEach(func() {
		clients = &mocks.ClientFactory{}
		kc = &mocks.Consumer{}

		// there is an odd case where CommitOffsets can be called if the goroutine of the markerconsumer runs fast enough
		// and reads 500 messages as per the config: RedeliveryNumOffsetsPerCommit. We want to specify that we do not
		// care about CommitOffsets being called since that detail is not pertinent to this test.
		// Addresses issue: https://jira.splunk.com/browse/DSP-16125
		kc.On("CommitOffsets", mock.Anything).Return(nil, nil).Maybe()

		kpMessageSender = &mocks.Producer{}
		kpMarkerProducer = &mocks.Producer{}
		mc = nil
		err = nil
	})

	AfterEach(func() {
		clients.AssertExpectations(GinkgoT())
		kc.AssertExpectations(GinkgoT())
		kpMessageSender.AssertExpectations(GinkgoT())
		kpMarkerProducer.AssertExpectations(GinkgoT())
	})

	Describe("NewMarkerConsumer", func() {
		JustBeforeEach(func() {
			mc, err = NewMarkerConsumer(validOpts, clients)
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
				clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
				clients.On("NewConsumer", mock.Anything).Once().Return(kc, nil)

				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil)
			})
			It("should create a new marker consumer", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
		When("a message sender cannot be constructed", func() {
			BeforeEach(func() {
				clients.On("NewProducer", kpMessageSenderCfg).Once().Return(nil, criticalError)
			})
			It("should fail and propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mc).Should(BeNil())
			})
		})
		When("a marker producer cannot be constructed", func() {
			BeforeEach(func() {
				clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
				clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(nil, criticalError)
			})
			It("should fail and propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mc).Should(BeNil())
			})
		})
		When("a kafka consumer cannot be constructed", func() {
			BeforeEach(func() {
				clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
				clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
				clients.On("NewConsumer", mock.Anything).Once().Return(nil, criticalError)
			})
			It("should fail and propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mc).Should(BeNil())
			})
		})
		When("subscribing to the marker topic fails", func() {
			BeforeEach(func() {
				clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
				clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
				clients.On("NewConsumer", mock.Anything).Once().Return(kc, nil)

				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(criticalError)
			})
			It("should fail and propagate the error", func() {
				Expect(err).Should(BeCausedBy(criticalError))
				Expect(mc).Should(BeNil())
			})
		})
	})

	Describe("RebalanceEvent handling", func() {
		var (
			partition = confluentKafka.TopicPartition{
				Topic:     &validOpts.MarkerTopic,
				Partition: 1,
			}
		)
		BeforeEach(func() {
			clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
			clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
			clients.On("NewConsumer", mock.Anything).Once().Return(kc, nil)
		})
		JustBeforeEach(func() {
			mc, err = NewMarkerConsumer(validOpts, clients)
		})
		When("a partition is added", func() {
			BeforeEach(func() {
				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
					rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

					var consumer *confluentKafka.Consumer
					ap := confluentKafka.AssignedPartitions{
						Partitions: []confluentKafka.TopicPartition{
							partition,
						},
					}
					rebalanceCb(consumer, ap)
				})
			})
			It("should add an assigned partition", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
		When("a partition is added that is already being handled", func() {
			BeforeEach(func() {
				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
					rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

					var consumer *confluentKafka.Consumer
					ap := confluentKafka.AssignedPartitions{
						Partitions: []confluentKafka.TopicPartition{
							partition,
						},
					}
					// the first callback adds the partition
					// the second callback short-circuits and returns
					rebalanceCb(consumer, ap)
					rebalanceCb(consumer, ap)
				})
			})
			It("should not add an assigned partition", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
		When("a partition is revoked", func() {
			BeforeEach(func() {
				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
					rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

					var consumer *confluentKafka.Consumer
					ap := confluentKafka.AssignedPartitions{
						Partitions: []confluentKafka.TopicPartition{
							partition,
						},
					}
					rp := confluentKafka.RevokedPartitions{
						Partitions: []confluentKafka.TopicPartition{
							partition,
						},
					}
					// the first callback adds a new partition
					// the second callback removes the partition
					rebalanceCb(consumer, ap)
					rebalanceCb(consumer, rp)
				})
			})
			It("should remove an assigned partition", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
		When("a partition is revoked that is not being handled", func() {
			BeforeEach(func() {
				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
					rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

					var consumer *confluentKafka.Consumer
					rp := confluentKafka.RevokedPartitions{
						Partitions: []confluentKafka.TopicPartition{
							partition,
						},
					}
					// this callback short-circuits and returns
					rebalanceCb(consumer, rp)
				})
			})
			It("should nto remove an assigned partition", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
		When("a non-rebalance event is received", func() {
			BeforeEach(func() {
				kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
					rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

					var consumer *confluentKafka.Consumer
					nonRebalanceEvent := confluentKafka.PartitionEOF{}

					// this callback short-circuits and returns
					rebalanceCb(consumer, nonRebalanceEvent)
				})
			})
			It("should nto remove an assigned partition", func() {
				Expect(err).ShouldNot(HaveOccurred())
				Expect(mc).ShouldNot(BeNil())
			})
		})
	})

	Describe("Message consumption when no partitions assigned", func() {
		var (
			readTimeout = 1 * time.Second
		)
		BeforeEach(func() {
			clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
			clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
			clients.On("NewConsumer", mock.Anything).Once().Return(kc, nil)

			kpMessageSender.On("Close").Once().Return()
			kpMarkerProducer.On("Close").Once().Return()

			kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil)
			kc.On("Close").Once().Return(nil)

			mc, err = NewMarkerConsumer(validOpts, clients)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mc).ShouldNot(BeNil())
		})
		JustBeforeEach(func() {
			mc.Start()
		})
		JustAfterEach(func() {
			mc.Stop()
		})
		When("reading a message fails with a timeout", func() {
			BeforeEach(func() {
				err := confluentKafka.NewError(confluentKafka.ErrTimedOut, "timed out", false)
				kc.On("ReadMessage", readTimeout).Return(nil, err)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message fails with a generic error", func() {
			BeforeEach(func() {
				err := confluentKafka.NewError(confluentKafka.ErrFail, "failed", false)
				kc.On("ReadMessage", readTimeout).Return(nil, err)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message succeeds", func() {
			BeforeEach(func() {
				msg := &confluentKafka.Message{}
				kc.On("ReadMessage", readTimeout).Return(msg, nil)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
	})

	Describe("Message consumption when one partition assigned", func() {
		var (
			partition = confluentKafka.TopicPartition{
				Topic:     &validOpts.MarkerTopic,
				Partition: 1,
			}
			readTimeout = 1 * time.Second
		)
		BeforeEach(func() {
			clients.On("NewProducer", kpMessageSenderCfg).Once().Return(kpMessageSender, nil)
			clients.On("NewProducer", kpMarkerProducerCfg).Once().Return(kpMarkerProducer, nil)
			clients.On("NewConsumer", mock.Anything).Once().Return(kc, nil)

			kpMessageSender.On("Close").Once().Return()
			kpMarkerProducer.On("Close").Once().Return()

			kc.On("Subscribe", validOpts.MarkerTopic, mock.Anything).Once().Return(nil).Run(func(args mock.Arguments) {
				rebalanceCb := args.Get(1).(confluentKafka.RebalanceCb)

				var consumer *confluentKafka.Consumer
				ap := confluentKafka.AssignedPartitions{
					Partitions: []confluentKafka.TopicPartition{
						partition,
					},
				}
				rebalanceCb(consumer, ap)
			})
			kc.On("Close").Once().Return(nil)

			mc, err = NewMarkerConsumer(validOpts, clients)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(mc).ShouldNot(BeNil())
		})
		JustBeforeEach(func() {
			mc.Start()
		})
		JustAfterEach(func() {
			mc.Stop()
		})
		When("reading a message fails with a timeout", func() {
			BeforeEach(func() {
				err := confluentKafka.NewError(confluentKafka.ErrTimedOut, "timed out", false)
				kc.On("ReadMessage", readTimeout).Return(nil, err)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message fails with a generic error", func() {
			BeforeEach(func() {
				err := confluentKafka.NewError(confluentKafka.ErrFail, "failed", false)
				kc.On("ReadMessage", readTimeout).Return(nil, err)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message for a partition that has not been assigned", func() {
			BeforeEach(func() {
				msg := &confluentKafka.Message{}
				kc.On("ReadMessage", readTimeout).Return(msg, nil)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message that is malformed", func() {
			BeforeEach(func() {
				msg := &confluentKafka.Message{
					TopicPartition: partition,
					Value:          []byte("this is not a valid marker"),
				}
				kc.On("ReadMessage", readTimeout).Return(msg, nil)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		When("reading a message that is malformed", func() {
			BeforeEach(func() {
				msg := &confluentKafka.Message{
					TopicPartition: partition,
					Value:          []byte("this is not a valid marker"),
				}
				kc.On("ReadMessage", readTimeout).Return(msg, nil)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
		Context("Happy path", func() {
			BeforeEach(func() {
				marker := kafkamq.Marker{
					Type:  kafkamq.Marker_START,
					Value: []byte("serialized task value"),
					Key:   []byte("job id"),
					MessageId: &kafkamq.MessageID{
						Partition: partition.Partition,
						Offset:    int64(0),
					},
					RedeliverAfterMs: 600,
				}
				var data []byte
				data, err = marker.Marshal()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(data).ShouldNot(BeNil())

				msg := &confluentKafka.Message{
					TopicPartition: partition,
					Key:            marker.Key,
					Value:          data,
				}
				kc.On("ReadMessage", readTimeout).Return(msg, nil)
			})
			It("should not add a message to a partiton", func() {
				// assertions tested on mocks
				Eventually(func() bool {
					return methodWasCalled(&kc.Mock, "ReadMessage", readTimeout)
				}).Should(BeTrue())
			})
		})
	})
})
