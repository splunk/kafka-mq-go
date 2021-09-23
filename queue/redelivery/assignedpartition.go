package redelivery

import (
	"fmt"
	"time"

	"github.com/splunk/kafka-mq-go/pkg/logging"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

var (
	zeroTime = time.Time{}
)

// AssignedPartition allows the marker consumer to track consumption progress
// as well as which messages require redelivery for a specific partition in the marker topic
type AssignedPartition interface {
	// AddMessage adds a marker message to this partition.
	// `now` is the local, monotonic clock time
	AddMessage(msg *confluentKafka.Message, now time.Time) error

	// SmallestMarkerOffset returns the smallest offset of all marker messages currently being tracked
	SmallestMarkerOffset() (int64, bool)

	// Redeliver determines which messages have expired redelivery deadlines and initiates redelivery.
	// `now` is the local, monotonic clock time
	Redeliver(now time.Time)

	// Close releases the resources consumed by this partition instance
	Close()
}

// AssignedPartition data structure
// Note that this data structure is not thread-safe, it is meant to be used by the marker consumer thread only
type assignedpartition struct {
	markersQueue        MarkersQueue // tracks markers for this partition
	redeliverer         Redeliverer  // for performing redelivery
	lastMarkerSeenAt    time.Time    // time that the last marker message was received (local monotonic clock time)
	lastMarkerTimestamp time.Time    // timestamp of the last marker message that was received

	// If a marker message is received before this duration elapses, use the timestamp of the last marker to determine
	// the messages that require redelivery.
	// Otherwise, fall back to calculating a synthetic marker timestamp by adding the duration that has elapsed since
	// the last marker was seen (based on the local monotonic clock) to the timestamp of the last marker.
	durationUseNowIfNoMarkerSeen time.Duration
	logger                       *logging.Logger
}

// NewAssignedPartition constructs a new AssignedPartiton instance
func NewAssignedPartition(markersQueue MarkersQueue, redeliverer Redeliverer, durationUseNowIfNoMarkerSeen time.Duration) AssignedPartition {
	logger := logging.With(logging.ComponentKey, "assignedpartition", partitionLogLabel, markersQueue.Partition())
	logger.Info("creating new assigned partition", "durationUseNowIfNoMarkerSeen", durationUseNowIfNoMarkerSeen)

	return &assignedpartition{
		markersQueue:                 markersQueue,
		redeliverer:                  redeliverer,
		lastMarkerTimestamp:          zeroTime,
		lastMarkerSeenAt:             zeroTime,
		durationUseNowIfNoMarkerSeen: durationUseNowIfNoMarkerSeen,
		logger:                       logger,
	}
}

// AddMessage adds a marker message to this partition.
// `now` is the local, monotonic clock time
func (ap *assignedpartition) AddMessage(msg *confluentKafka.Message, now time.Time) error {
	if msg == nil {
		return errors.New("message must not be nil")
	}

	ap.logger.Debug("add message", msgLogKey, fmt.Sprintf("%+v", *msg))

	partition := msg.TopicPartition.Partition
	if partition != ap.markersQueue.Partition() {
		return errors.Errorf("received a message for a different partition")
	}

	markerOffset := int64(msg.TopicPartition.Offset)
	markerTimestamp := msg.Timestamp

	marker := &kafkamq.Marker{}
	err := marker.Unmarshal(msg.Value)
	if err != nil {
		return errors.Wrap(err, "Could not unmarshal marker from message")
	}

	ap.markersQueue.AddMarker(markerOffset, markerTimestamp, marker)

	ap.lastMarkerTimestamp = markerTimestamp
	ap.lastMarkerSeenAt = now

	return nil
}

// SmallestMarkerOffset returns the smallest offset of all marker messages currently being tracked
func (ap *assignedpartition) SmallestMarkerOffset() (int64, bool) {
	return ap.markersQueue.SmallestMarkerOffset()
}

// Redeliver determines which messages have expired redelivery deadlines and initiates redelivery.
// `now` is the local, monotonic clock time
func (ap *assignedpartition) Redeliver(now time.Time) {
	redeliverTimestamp := ap.getRedeliverTimestamp(now)

	if redeliverTimestamp.IsZero() {
		// We haven't seen any markers yet, no need to check for redelivery
		return
	}

	markers := ap.markersQueue.MarkersToRedeliver(redeliverTimestamp)
	ap.redeliverer.Redeliver(markers)
}

// Close releases the resources consumed by this partition instance
func (ap *assignedpartition) Close() {
	ap.logger.Info("stopping redeliverer")
	ap.redeliverer.Stop()
}

// getRedeliverTimestamp returns the time that should be used when determining which messages
// require redelivery
func (ap *assignedpartition) getRedeliverTimestamp(now time.Time) time.Time {
	if ap.lastMarkerSeenAt.IsZero() {
		// We haven't seen any markers yet
		return zeroTime
	}

	// If we've seen a marker recently (within `durationUseNowIfNoMarkerSeen`), use the timestamp
	// of the last marker for computing which messages require redelivery
	durationNoMarkerSeen := now.Sub(ap.lastMarkerSeenAt)
	if durationNoMarkerSeen < ap.durationUseNowIfNoMarkerSeen {
		// We've seen a marker recently
		return ap.lastMarkerTimestamp
	}

	// We haven't seen a marker recently.
	// Compute a logical marker timestamp by adding the time that has elapsed since
	// we saw the last marker (based on the local monotonic clock) to the timestamp of the last marker.
	return ap.lastMarkerTimestamp.Add(durationNoMarkerSeen)
}
