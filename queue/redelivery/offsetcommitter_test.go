package redelivery_test

import (
	"errors"

	"github.com/splunk/kafka-mq-go/pkg/kafka/mocks"
	"github.com/splunk/kafka-mq-go/queue/redelivery"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	partition = 0
)

var _ = Describe("OffsetCommitter", func() {
	const (
		numOffsetsPerCommit = 3
	)
	var (
		oc            redelivery.OffsetCommitter
		kafkaConsumer *mocks.Consumer
		criticalError = errors.New("oops")
	)

	BeforeEach(func() {
		kafkaConsumer = &mocks.Consumer{}

		oc = redelivery.NewOffsetCommitter(markerTopic, numOffsetsPerCommit, kafkaConsumer)
		Expect(oc).ShouldNot(BeNil())
	})

	AfterEach(func() {
		kafkaConsumer.AssertExpectations(GinkgoT())
	})

	Context("AddMarker", func() {
		When("less than numOffsetsPerCommit marker offsets have been added", func() {
			It("should not attempt to commit", func() {
				addMarkers(oc, 2)
			})
		})
		When("when numOffsetsPerCommit marker offsets have been added in order", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsets, nil)
			})
			It("should attempt to commit", func() {
				addMarkers(oc, 3)
			})
		})
		When("when less than two times numOffsetsPerCommit marker offsets have been added", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsets, nil)
			})
			It("should attempt to commit once", func() {
				addMarkers(oc, 5)
			})
		})
		When("when two times numOffsetsPerCommit marker offsets have been added", func() {
			JustBeforeEach(func() {
				offsets1 := newTopicPartitions(map[int]int{
					partition: 2,
				})
				offsets2 := newTopicPartitions(map[int]int{
					partition: 5,
				})
				kafkaConsumer.On("CommitOffsets", offsets1).Once().Return(offsets1, nil)
				kafkaConsumer.On("CommitOffsets", offsets2).Once().Return(offsets2, nil)
			})
			It("should attempt to commit twice", func() {
				addMarkers(oc, 6)
			})
		})
		When("when numOffsetsPerCommit marker offsets have been added out of order", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsets, nil)
			})
			It("should attempt to commit", func() {
				oc.AddMarker(redelivery.NewMarkerID(partition, 2))
				oc.AddMarker(redelivery.NewMarkerID(partition, 1))
				oc.AddMarker(redelivery.NewMarkerID(partition, 0))
			})
		})
		When("when commit of all offsets fails", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(nil, criticalError)
			})
			It("should log the error", func() {
				addMarkers(oc, 3)
			})
		})
		When("when commit of an individual offset fails", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				offsetsWithErr := make([]confluentKafka.TopicPartition, len(offsets))
				copy(offsetsWithErr, offsets)
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsetsWithErr, nil)
			})
			It("should log the error", func() {
				addMarkers(oc, 3)
			})
		})
		When("when commit of an individual offset is not performed", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 2,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return([]confluentKafka.TopicPartition{}, nil)
			})
			It("should log the error", func() {
				addMarkers(oc, 3)
			})
		})
	})

	Context("DropOffsets", func() {
		When("no offsets have been added", func() {
			It("should succeed", func() {
				oc.DropOffsets(partition)
			})
		})
		When("offsets for a partition have been added", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 0,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsets, nil)
			})
			It("should succeed", func() {
				// add offsets 0, 1
				addMarkers(oc, 2)

				// drop
				oc.DropOffsets(partition)

				// add offsets 0, 1 again and a commit should occur for 0
				addMarkers(oc, 2)
			})
		})
		When("offsets for a different partition have been added", func() {
			JustBeforeEach(func() {
				offsets := newTopicPartitions(map[int]int{
					partition: 1,
				})
				kafkaConsumer.On("CommitOffsets", offsets).Once().Return(offsets, nil)
			})
			It("should succeed", func() {
				// add offsets 0, 1
				addMarkers(oc, 2)

				// drop a different partition
				oc.DropOffsets(partition + 1)

				// add offsets 0, 1 again and a commit should occur for 1
				addMarkers(oc, 2)
			})
		})
	})
})

func addMarkers(oc redelivery.OffsetCommitter, numOffsets int) {
	for i := 0; i < numOffsets; i++ {
		oc.AddMarker(redelivery.NewMarkerID(partition, int64(i)))
	}
}

func newTopicPartitions(offsets map[int]int) []confluentKafka.TopicPartition {
	ret := []confluentKafka.TopicPartition{}
	for partition, offset := range offsets {
		ret = append(ret, confluentKafka.TopicPartition{
			Topic:     &markerTopic,
			Partition: int32(partition),
			Offset:    confluentKafka.Offset(offset),
		})
	}
	return ret
}
