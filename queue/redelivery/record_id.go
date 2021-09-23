package redelivery

import (
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"
)

// RecordID uniqely identifies a record in a kafka topic by using
// the record's location (partition:offset)
type RecordID struct {
	partition int32
	offset    int64
}

// MessageID uniquely identifies a record from the queue topic
type MessageID = RecordID

// MarkerID uniquely identifies a record from the marker topic
type MarkerID = RecordID

// MessageIDFromMarker returns an identifier that uniquely identifies the message record
// from the queue topic that the specified `marker` tracks.
func MessageIDFromMarker(marker *kafkamq.Marker) MessageID {
	return NewMessageID(marker.MessageId.Partition, marker.MessageId.Offset)
}

// NewMessageID constructs an id that uniquely identifies a message record.
func NewMessageID(partition int32, offset int64) MessageID {
	return MessageID{
		partition: partition,
		offset:    offset,
	}
}

// NewMarkerID constructs an id that uniquely identifies a marker record.
func NewMarkerID(partition int32, offset int64) MarkerID {
	return MarkerID{
		partition: partition,
		offset:    offset,
	}
}
