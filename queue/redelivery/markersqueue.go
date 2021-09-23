package redelivery

import (
	"time"

	"github.com/splunk/kafka-mq-go/pkg/logging"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"

	"github.com/pkg/errors"
)

const (
	markerLogLabel             = "marker"
	msgIDLogLabel              = "msgID"
	markerOffsetLogLabel       = "markerOffset"
	markerTimestampLogLabel    = "markerTimestamp"
	redeliveryDeadlineLogLabel = "redeliveryDeadline"
)

var (
	errIllegalState = errors.New("illegal state error")
)

// MarkersQueue is the datastructure used to track Markers consumed from the markers topic that determines:
// 1. Which messages currently are in-progress, including their payload.
// 2. Which messages require redelivery (based on marker redelivery deadlines)
// 3. The offset up to which markers have been consumed from the markers
//    topic; this offset is used when committing marker consumer offsets
//    to Kafka.
//
// Note that the MarkersQueue is not thread-safe as it is only meant to be used by a single
// Kafka consumer thread.
type MarkersQueue interface {
	// Partition returns the ordinal of the partition that the queue is tracking
	Partition() int32

	// AddMarker adds a marker to the queue.
	AddMarker(offset int64, timestamp time.Time, marker *kafkamq.Marker)

	// MarkersToRedelivery returns the markers that require redelivery given the specified timestamp.
	MarkersToRedeliver(now time.Time) []*kafkamq.Marker

	// SmallestMarkerOffset returns the smallest marker offset tracked by the queue.
	// Returns `(0, false)` if the queue is empty.
	SmallestMarkerOffset() (int64, bool)
}

type markersQueue struct {
	partition         int32
	markersInProgress map[MessageID]*kafkamq.Marker
	markersByDeadline *MarkerDeadlinesPQ
	markersByOffset   *MarkerOffsetsPQ
	logger            *logging.Logger
}

// NewMarkersQueue constructs a new MarkersQueue instance
func NewMarkersQueue(partition int32) MarkersQueue {
	logger := logging.With(logging.ComponentKey, "markersqueue", "partition", partition)
	logger.Info("creating new markers queue")

	return &markersQueue{
		partition:         partition,
		markersInProgress: make(map[MessageID]*kafkamq.Marker),
		markersByDeadline: NewMarkerDeadlinesPQ(),
		markersByOffset:   NewMarkerOffsetsPQ(),
		logger:            logger,
	}
}

// Partition returns the ordinal of the partition that the queue is tracking
func (q *markersQueue) Partition() int32 {
	return q.partition
}

// AddMarker a marker to the queue.  Where `offset` is the offset of the marker record consumed from the markers topic
// and `timestamp` is the timestamp of the marker record consumed from the markers topic.
func (q *markersQueue) AddMarker(offset int64, timestamp time.Time, marker *kafkamq.Marker) {
	q.logger.Debug("adding marker", markerOffsetLogLabel, offset, markerTimestampLogLabel, timestamp, markerLogLabel, marker)

	msgID := MessageIDFromMarker(marker)
	switch marker.Type {
	case kafkamq.Marker_START:
		// Add an entry for the marker to the in-progress table and track it in the priority queues.
		_, exists := q.markersInProgress[msgID]
		if exists {
			// It is possible to get multiple start markers for the same message when:
			// 1. The consumer's call to mqclient.NextMessage sends a start marker, but fails to commit offset.
			//    The next attempt to consume will send a new start marker and attempt to commit again.
			// In this case, it is safe to update the re-delivery deadline associated with the latest start marker.
			// We SHOULD NOT update the commit offset of the start marker entry, as there may be other
			// markers which have been queued after it.
			q.logger.Debug("duplicate start marker", msgIDLogLabel, msgID)

			redeliveryDeadline := timestamp.Add(time.Duration(marker.RedeliverAfterMs) * time.Millisecond)

			q.logger.Debug("updating redelivery deadline", redeliveryDeadlineLogLabel, redeliveryDeadline)
			if ok := q.markersByDeadline.Update(msgID, redeliveryDeadline); !ok {
				q.logger.Fatal(errIllegalState, "start marker tracked as in-progress but entry could not be found in the markersByDeadline PQ", markerLogLabel, marker)
			}
		} else {
			q.logger.Debug("new start marker", msgIDLogLabel, msgID)
			q.logger.Debug("enqueueing marker offset", markerOffsetLogLabel, offset)
			if ok := q.markersByOffset.Enqueue(MarkerOffset{MessageID: msgID, OffsetOfMarker: offset}); !ok {
				q.logger.Info("marker offset already exists for marker, ignoring", msgIDLogLabel, msgID)
			}

			redeliveryDeadline := timestamp.Add(time.Duration(marker.RedeliverAfterMs) * time.Millisecond)

			q.logger.Debug("enqueuing marker deadline", redeliveryDeadlineLogLabel, redeliveryDeadline)
			if ok := q.markersByDeadline.Enqueue(MarkerDeadline{MessageID: msgID, RedeliveryDeadline: redeliveryDeadline}); !ok {
				q.logger.Info("marker deadline already exists for marker, ignoring", msgIDLogLabel, msgID)
			}

			q.logger.Debug("adding entry to markers in-progress map", msgIDLogLabel, msgID)
			q.markersInProgress[msgID] = marker
		}
	case kafkamq.Marker_END:
		// Remove the marker from the in-progress table.
		// The marker will be pruned from the priority queues when determining:
		// - the set of markers to redeliver
		// - the smallest marker offset
		q.logger.Debug("new end marker", msgIDLogLabel, msgID)
		delete(q.markersInProgress, msgID)
	case kafkamq.Marker_KEEPALIVE:
		// Update the redelivery deadline associated with the marker and adjust the priority queue
		q.logger.Debug("new keep-alive marker", msgIDLogLabel, msgID)
		_, exists := q.markersInProgress[msgID]
		if !exists {
			// If an entry doesn't exist for the maker associated with the keep alive we have either:
			// 1. Seen an end marker that removed it from the in-progress table.
			//    The queue consumer prevents this by stopping the keep-alive thread for a message synchronously before sending the end marker.
			// 2. Not seen a start marker to add it to the in-progress table.
			//    The queue consumer prevents this by synchrously sending a start marker before starting up the keep-alive thread.
			// In both of the above cases, if a bugs wer introduced in the consumer, we don't want a straggling or out-of-order keep-alive
			// to bring a marker to life and cause repeated re-delivery of a message.
			q.logger.Info("entry for marker does not exist in in-progress map, ignoring", msgIDLogLabel, msgID)
			return
		}

		redeliveryDeadline := timestamp.Add(time.Duration(marker.RedeliverAfterMs) * time.Millisecond)
		q.logger.Debug("updating redelivery deadline", redeliveryDeadlineLogLabel, redeliveryDeadline)

		if ok := q.markersByDeadline.Update(msgID, redeliveryDeadline); !ok {
			q.logger.Warn("keep-alive marker tracked as in-progress but entry could not be found in the markersByDeadline PQ. This message can be duplicated", markerLogLabel, msgID)
		}
	}
}

// MarkersToRedeliver determines what markers require redelivery given the specified timestamp.
func (q *markersQueue) MarkersToRedeliver(now time.Time) []*kafkamq.Marker {
	// Dequeue markers that are no longer in progress
	for q.isMarkersByDeadlineHeadEnded() {
		q.markersByDeadline.Dequeue()
	}

	// Determine what markers require redelivery
	var toRedeliver = []*kafkamq.Marker{}

	for {
		head, ok := q.markersByDeadline.Head()
		if !ok {
			// queue is empty
			break
		}
		if !now.After(head.RedeliveryDeadline) {
			// redelivery deadline has not passed
			break
		}

		// At this point, we know that the deadline for the head has expired, so dequeue it.
		item, ok := q.markersByDeadline.Dequeue()
		if !ok {
			q.logger.Fatal(errIllegalState, "failed to dequeue from the markersByDeadline PQ even though the head was non-empty")
		}

		// Check whether the marker is still in progress:
		// - If it is, it requires redelivery
		// - If it isn't. we've received an end marker and redelivery is not required
		if m, exists := q.markersInProgress[item.MessageID]; exists {
			toRedeliver = append(toRedeliver, m)
		}

		// At this point, the marker has been removed from the markersByDeadline queue regardless of
		// whether redelivery is required.  However, we leave an entry for the marker in the
		// markersInProgress map until we are sure that the message has been redelivered
		// (the re-delivery code produces an end marker when it is done).
		// Also, the markersInProgress entry needs to stay for the code in `SmallestMarkerOffset`
		// which calculates the minium marker offset to be correct.
	}

	return toRedeliver
}

// SmallestMarkerOffset returns the smallest marker offset tracked by the queue.
// Returns `(0, false)` if the queue is empty.
func (q *markersQueue) SmallestMarkerOffset() (int64, bool) {
	// Dequeue markers that are no longer in progress
	for q.isMarkersByOffsetHeadEnded() {
		q.markersByOffset.Dequeue()
	}

	head, ok := q.markersByOffset.Head()
	if !ok {
		return 0, false
	}

	return head.OffsetOfMarker, true
}

func (q *markersQueue) isMarkersByDeadlineHeadEnded() bool {
	head, ok := q.markersByDeadline.Head()
	if !ok {
		return false
	}
	return q.isEnded(head.MessageID)
}

func (q *markersQueue) isMarkersByOffsetHeadEnded() bool {
	head, ok := q.markersByOffset.Head()
	if !ok {
		return false
	}
	return q.isEnded(head.MessageID)
}

func (q *markersQueue) isEnded(messageID MessageID) bool {
	_, exists := q.markersInProgress[messageID]
	return !exists
}
