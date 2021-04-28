package redelivery

import (
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka"
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

const (
	offsetLogLabel    = "offset"
	partitionLogLabel = "partition"
)

// OffsetCommitter allows the redelivery tracker to periodically commit batches of marker offsets to Kafka
type OffsetCommitter interface {
	// AddMarker registers a marker to be committed
	AddMarker(MarkerID)

	// DropOffsets drops any cached offsets for the specified partition
	DropOffsets(partition int32)
}

// OffsetCommitter data structure
// Note that the offset committer is not thread-safe, it is meant to be used by the marker consumer thread only
type offsetcommitter struct {
	markerTopic         string
	numOffsetsPerCommit uint
	kConsumer           kafka.Consumer
	offsetsToCommit     map[int32]int64
	offsetCounter       uint
	logger              *logging.Logger
}

// NewOffsetCommitter constructs a new OffsetCommitter instance that can be used to periodically commit
// consumer offsets for the specified `markerTopic`.
// `numOffsetsPerCommit` is the number of offsets that must be added before a commit is performed.
func NewOffsetCommitter(markerTopic string, numOffsetsPerCommit uint, kConsumer kafka.Consumer) OffsetCommitter {
	logger := logging.With(logging.ComponentKey, "offsetcommitter", "markerTopic", markerTopic)
	logger.Info("creating new offset committer", "numOffsetsPerCommit", numOffsetsPerCommit)

	return &offsetcommitter{
		markerTopic:         markerTopic,
		numOffsetsPerCommit: numOffsetsPerCommit,
		kConsumer:           kConsumer,
		offsetsToCommit:     make(map[int32]int64),
		offsetCounter:       0,
		logger:              logger,
	}
}

// AddMarker registers a marker to be commited
func (oc *offsetcommitter) AddMarker(markerID MarkerID) {

	partition := markerID.partition
	offset := markerID.offset

	oc.logger.Debug("adding offset", partitionLogLabel, partition, offsetLogLabel, offset)

	// Update the offset to commit map
	if currOffset, ok := oc.offsetsToCommit[partition]; !ok {
		oc.offsetsToCommit[partition] = offset
	} else {
		// Update existing entries in the map if the incoming offset is larger
		if currOffset < offset {
			oc.offsetsToCommit[partition] = offset
		}
	}

	// Attempt to commit offsets once we have a large enough batch
	// Commtting offsets is an expensive operation as it requires communication with ZooKeeper.
	oc.offsetCounter = (oc.offsetCounter + 1) % oc.numOffsetsPerCommit
	if oc.offsetCounter == 0 {
		err := oc.commitOffsets()
		if err != nil {
			// In the event that offset commit fails the best we can do is log
			// because offset commit will not be retried until another batch of
			// offsets are received.
			oc.logger.Error(err, "failed to commit offsets")
		}

		// Reset the offsetsToCommit map because offset commit will not be retried
		oc.offsetsToCommit = make(map[int32]int64)
	}
}

// DropOffsets drops any cached offsets for the specified partition
func (oc *offsetcommitter) DropOffsets(partition int32) {
	oc.logger.Debug("dropping offsets for partition", partitionLogLabel, partition)
	delete(oc.offsetsToCommit, partition)
}

func (oc *offsetcommitter) commitOffsets() error {
	oc.logger.Debug("attempting to commit offsets")

	offsets := []confluentKafka.TopicPartition{}
	for partition, offset := range oc.offsetsToCommit {
		offsets = append(offsets, confluentKafka.TopicPartition{
			Topic:     &oc.markerTopic,
			Partition: partition,
			Offset:    confluentKafka.Offset(offset),
		})
	}

	committedOffsets, err := oc.kConsumer.CommitOffsets(offsets)
	if err != nil {
		return errors.Wrapf(err, "failed to commit all offsets %+v", offsets)
	}

	// Verify the set of committed offsets.  Offset commits may fail for specific partitions if they are attempted
	// during or after a consumer rebalance; in this case, the kConsumer will no longer be able to commit to partitions
	// that were assigned to other redelivery tracker instances.
	// If an error occurs during offset commit the best we can do is log.
	for _, committed := range committedOffsets {
		if committed.Error != nil {
			oc.logger.Warn("failed to commit individual offset", offsetLogLabel, committed, logging.ErrorKey, committed.Error)
		}
		delete(oc.offsetsToCommit, committed.Partition)
	}
	for partition, offset := range oc.offsetsToCommit {
		oc.logger.Warn("requested commit of individual offset was not performed", partitionLogLabel, partition, offsetLogLabel, offset)
	}

	return nil
}
