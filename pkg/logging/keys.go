package logging

const (
	// ErrorKey is used to associate an error with the a message
	ErrorKey = "error"

	// ComponentKey is used to associate a component or class with a log message
	ComponentKey = "component"

	// CallstackKey is used to associate a callstack with a log message
	CallstackKey = "callstack"

	// HostnameKey is used to associate a hostname with a log message
	HostnameKey = "hostname"

	// LevelKey is used to associate a level with a log message
	LevelKey = "level"

	// LocationKey is used to associate a location with a log message
	LocationKey = "location"

	// MessageKey is used to associate a message with a log message
	MessageKey = "message"

	// ServiceKey is used to associate a service with a log message
	ServiceKey = "service"

	// TimeKey is used to associate a time with a log message
	TimeKey = "time"

	// QueueID is used to associate a queue identifier with a log message
	QueueID = "queueID"

	// Topic is used to associate a topic name with a log message
	Topic = "topic"

	// Partition is used to associate a partition id with a log message
	Partition = "partition"

	// Offset is used to associate a Kafka message offset with a log message
	Offset = "offset"
)
