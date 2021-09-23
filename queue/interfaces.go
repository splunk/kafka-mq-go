package queue

import (
	"context"
	"io"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
)

// Message represents a message received by a Queue Consumer
type Message interface {

	// Payload returns the message payload
	Payload() []byte

	// Ack notifies that message processing has been completed.
	// This message will never be delivered again.
	Ack() error

	// Nack notifies that message processing has failed.
	// This message will never be delivered again.
	Nack() error

	// Interrupt notifies that message processing has been interrupted.
	// The message will be made visible to other Queue Consumers.
	Interrupt() error
}

// Event is a delivery notification event that is returned on the
// `deliveryCh` provided to Producer.SendAsync (see below)
type Event = kafka.Event

// MessageDeliveredEvent signifies that a message has been delivered
type MessageDeliveredEvent = kafka.MessageDeliveredEvent

// ErrorEvent signifies that delivery of a message has failed
type ErrorEvent = kafka.ErrorEvent

// Producer provides a means to sending messages to a Queue.
type Producer interface {
	// Close all resources needed to send messages.
	io.Closer

	// Send a message to the queue.
	// This is a synchronous, blocking call.
	Send(payload []byte) error

	// Send a message asynchronously to the queue with delivery notifications
	// returned on the specified channel
	SendAsync(payload []byte, deliveryCh chan Event)

	// Flush all messages queued for send and block until either all have been sent
	// or the specified context completes, whichever comes first.
	Flush(ctx context.Context)
}

// Consumer provides a means to receive messages from a Queue.
type Consumer interface {
	// Close all resources needed to receive messages.
	io.Closer

	// Receive a message from the queue.
	// Returns `nil, nil` if the specified context completes before a message is received from the queue.
	Receive(ctx context.Context) (Message, error)
}

// Queue encapsulates the transport of messages sent by Producers and received by Consumers
type Queue interface {
	// Producer returns a new Producer instance that is capable of sending messages to this Queue.
	// The returned Producer must be closed to release resources after it is used.
	Producer() (Producer, error)

	// Consumer returns a new Consumer instance that is capable of receiving messages from this Queue.
	// The returned Consumer must be closed to release resources after it is used.
	Consumer() (Consumer, error)
}

// Service provides a means to create or get a `Queue`
type Service interface {
	// GetOrCreateQueue returns a `Queue` identified by the specified id.
	GetOrCreateQueue(id string) Queue
}
