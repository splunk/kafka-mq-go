package internal

import (
	"bytes"
	"sync"
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/pkg/logging"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// MqClient is an interface for a Kafka-based message queue client.
//
// A call to `NextMessage()` will:
//
// 1. read a message from the `MessageTopic`
// 2. send a start marker for the message to the `MarkerTopic` and wait until they are written
// 3. commit read offsets of the message `MessageTopic`
//
// The next step of the message flow - 4. processing the messages - should be done by the client.
//
// Optionally, while the message is being processed, the `ProcessingInProgress()` method can be called which will:
// 5. send a keep-alive marker to the `MarkerTopic`
//
// After the message is processed, the `Processed()` method should be called which will:
// 6. send an end marker to the `MarkerTopic`
//
// Note that both `ProcessingInProgress()` and `Processed()` can be called at any time for any message and out-of-order.
//
// In the event that message processing fails and should be retried the message must be re-delivered, and neither
// `ProcessingInProgress()` nor `Processed()` should be called.
type MqClient interface {
	// NextMessage returns the next message from the queue.
	NextMessage(timeout time.Duration) (*confluentKafka.Message, error)

	// ProcessingInProgress indicates that processing of `msg` is in progress by sending a keep-alive marker to the `MarkerTopic`
	ProcessingInProgress(msg *confluentKafka.Message) error

	// Processed indicates that processing of `msg` is complete by sending an end marker to the `MarkerTopic`
	Processed(msg *confluentKafka.Message) error

	// NeetsReset returns true if this mqclient is tainted and must be reset.
	// An mqclient becomes tainted when the current consumer offset and committed consumer offset for the message topic
	// become inconsistent with the markers that have been written to the marker topic.
	// Note 1: At any point in time it should be possible to reason about the contents of the marker topic by inspecting:
	// - the current consumer offset
	// - the committed consumer offset
	// Note 2: The opposite is also true; we should be able to reason about the contents current offsets in the message
	//         topic by inspecting the contents of the marker topic.
	NeedsReset() bool

	// Reset resets this mqclient back to the last message queue offset commited to Kafka
	// This function clears tainted state by ensuring that:
	// 1. the next message read from the kafka consumer will be from last committed offset
	// 2. the next marker written will be for the message read in 1.
	Reset() error

	// NeedsReconnect returns true if the underlying kafka consumer has become disconnected
	// (e.g due to idle timeout expiry) and requires reconnect
	NeedsReconnect() bool

	// Reconnect reconnects the underlying kafka consumer
	Reconnect() error

	// Close tears down the resources associated with this mqclient; it is unusable after this call.
	Close() error
}

// mqclient is an implementation of the MqClient interface; the implementation is thread-safe.
//
// A `sync.Mutex` (fgLock) is used to protect access to the underlying KafkaConsumer (which is not thread safe)
// to ensure that it is only ever accessed by a single thread at a time.
//
// A `sync.RWMutex` (bgLock) is used to protect access to the underlying KafkaProducer (which is thread safe) and will be called
// asynchrounously by multiple background worker threads as message processing is completed.
//
// In functions that access both the KafkaConsumer and KafkaProducer. the fgLock must be acquired before the bgLock
type mqclient struct {
	queueID        string              // the identifier of the queue that this client is consuming from
	queueKey       []byte              // the filter applied to message keys to determine whether the message belongs to this client
	config         MqClientConfig      // the mqclient configuration bundle
	kConsumer      kafka.Consumer      // kafka consumer for reading messages from the `MessageTopic`
	clients        kafka.ClientFactory // for re-constructing consumer instances in the event that they become disconnected
	markerProducer MarkerProducer      // for writing markers to the `MarkerTopic`
	needsReset     bool                // true if the consumer offsets need to be reset before the next read
	needsReconnect bool                // true if the consumer has become disconnected and requires reconnect
	closed         bool                // true if this client has been closed
	fgLock         sync.Mutex          // acquired by methods that are called from the foreground context: NextMessage(), Close(), IsTainted()
	bgLock         sync.RWMutex        // acquired by methods that are called asynchronously from the background context: Processed(), ProcessingInProgress()
	logger         *logging.Logger
}

// MqClientConfig is a bundle of configuration for an `MqClient` instance
type MqClientConfig struct {
	BootstrapServers     string        // kafka cluster connection information
	MessageTopic         string        // the name of the topic that stores messages
	MarkerTopic          string        // the name of the topic that stores markers
	RedeliveryTimeout    time.Duration // time after which messages should be redelivered
	MaxMessagesPerCommit uint          // the maximum number of messages consumed before offsets are committed
}

// NewMqClient returns a handle to an open `MqClient` instance.
func NewMqClient(clients kafka.ClientFactory, config MqClientConfig, queueID string) (MqClient, error) {
	logger := logging.WithComponent("MqClient").With(logging.QueueID, queueID)
	logger.Info("new mqclient", "config", config)

	kc, err := newKafkaConsumer(clients, config, queueID)
	if err != nil {
		return nil, err
	}

	mpCfg := MarkerProducerConfig{
		BootstrapServers: config.BootstrapServers,
		Topic:            config.MarkerTopic,
	}
	mp, err := NewMarkerProducer(clients, mpCfg)
	if err != nil {
		return nil, err
	}

	mq := &mqclient{
		queueID:        queueID,
		queueKey:       []byte(queueID),
		config:         config,
		kConsumer:      kc,
		clients:        clients,
		markerProducer: mp,
		needsReset:     false,
		needsReconnect: false,
		closed:         false,
		fgLock:         sync.Mutex{},
		bgLock:         sync.RWMutex{},
		logger:         logger,
	}

	if err := mq.Reset(); err != nil {
		return nil, err
	}

	return mq, nil
}

func (mq *mqclient) NextMessage(timeout time.Duration) (*confluentKafka.Message, error) {
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()

	mq.logger.Debug("mqclient NextMessage")

	// -2. Turn away calls to fetch the next message if the consumer has been closed
	if mq.closed {
		return nil, mq.errClosed()
	}

	// -1. Turn away calls to fetch the next message if this client requires a reconnect
	if mq.needsReconnect {
		return nil, errors.Errorf("mqclient for queue %s is disconnected, Reconnect before calling NextMessage", mq.queueID)
	}
	// 0. Turn away calls to fetch the next message if this client requires a reset
	if mq.needsReset {
		return nil, errors.Errorf("mqclient for queue %s is tainted, Reset before calling NextMessage", mq.queueID)
	}

	// 1. Read the next message for this queue
	//
	// Note: Multiple queues are multiplexed off of the same topic and we filter to messages of interest here.
	//
	// Consumer offsets are committed whenever one of the following happens:
	// - we read a message that belongs to the queue of interest
	// - we read `mqClientConfig.MaxMessagesPerCommit` messages that belong to other queues
	var numMessages uint
	var msg *confluentKafka.Message
	var err error
	for {
		msg, err = mq.consumerReadMessage(timeout)
		if err != nil {
			return nil, errors.Wrapf(err, "mqclient for queue %s failed to read message", mq.queueID)
		}
		if bytes.Equal(msg.Key, mq.queueKey) {
			// This is a message for our queue
			break
		}

		// This is a message for another queue
		numMessages = (numMessages + 1) % mq.config.MaxMessagesPerCommit
		if numMessages == 0 {
			if err := mq.consumerCommitSync(msg); err != nil {
				return nil, err
			}
		}

	}
	mq.logger.Debug("mqclient read next message", "msg", msg)

	// 2. Send a start marker for the message
	err = mq.markerProducer.SendStartMarker(mq.queueID, msg, mq.getRedeliveryTimeout())
	if err != nil {
		// Failure to send a marker here results in this mqclient becoming tainted because:
		// - the current offset of the consumer has advanced.
		// - the current `msg` is not tracked by the redelivery tracker.
		// The combination of the above means that will lose the current `msg` unless we rewind the consumer to the last committed offset.
		mq.needsReset = true
		return nil, err
	}
	mq.logger.Debug("mqclient start marker send success")

	// 3. Synchronously commit the current offsets
	if err = mq.consumerCommitSync(msg); err != nil {
		// Failure to commit offsets here results in this mqclient becoming tainted because:
		// - the current offset of the consumer has advanced.
		// - the current `msg` is tracked by the redelivery tracker.
		// - the committed offset of the consumer has not advanced.
		// The combination of the above means that will duplicate the current `msg` unless we rewind the consumer so that we can attempt the commit again.
		mq.needsReset = true
		return nil, err
	}

	return msg, nil
}

func (mq *mqclient) ProcessingInProgress(msg *confluentKafka.Message) error {
	mq.bgLock.RLock()
	defer mq.bgLock.RUnlock()

	mq.logger.Debug("mqclient ProcessingInProgress", "msg", msg)

	if mq.closed {
		return mq.errClosed()
	}

	return mq.markerProducer.SendKeepAliveMarker(mq.queueID, msg, mq.getRedeliveryTimeout())
}

func (mq *mqclient) Processed(msg *confluentKafka.Message) error {
	mq.bgLock.RLock()
	defer mq.bgLock.RUnlock()

	mq.logger.Debug("mqclient Processed", "msg", msg)

	if mq.closed {
		return mq.errClosed()
	}

	return mq.markerProducer.SendEndMarker(mq.queueID, msg)
}

func (mq *mqclient) NeedsReset() bool {
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()

	return mq.needsReset
}

func (mq *mqclient) NeedsReconnect() bool {
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()

	return mq.needsReconnect
}

func (mq *mqclient) Reset() error {
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()

	mq.logger.Info("mqclient Reset")
	if mq.closed {
		return mq.errClosed()
	}

	return mq.reset()
}

func (mq *mqclient) Reconnect() error {
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()

	mq.logger.Info("mqclient Reconnect")
	if mq.closed {
		return mq.errClosed()
	}

	// Close the existing consumer and create a new one
	if err := mq.consumerClose(); err != nil {
		// close failed, but we are about to create a new client so we log and ignore here
		mq.logger.Warn("failed to close kafka consumer, ignoring", logging.ErrorKey, err)
	}

	kc, err := newKafkaConsumer(mq.clients, mq.config, mq.queueID)
	if err != nil {
		return err
	}

	mq.kConsumer = kc

	if err := mq.reset(); err != nil {
		return err
	}

	mq.needsReconnect = false

	return nil
}

func (mq *mqclient) reset() error {
	// Consumer.Subscribe() performs an atomic replacement of the current partiton assignment, reseting the consumer group offset in the process
	mq.logger.Info("mqclient kafka consumer topic subscribe", "topic", mq.config.MessageTopic)
	err := mq.kConsumer.Subscribe(mq.config.MessageTopic, func(_ *confluentKafka.Consumer, e confluentKafka.Event) error {
		mq.logger.Info("mqclient kafka consumer received rebalance event", "event", e)
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "mqclient for queue %s failed to subscribe consumer to topic %s", mq.queueID, mq.config.MessageTopic)
	}

	mq.needsReset = false

	return nil
}

func (mq *mqclient) Close() error {
	mq.logger.Info("mqclient Close")

	// The foreground lock is acquired first and released last
	// NB: defers are executed in reverse order
	mq.fgLock.Lock()
	defer mq.fgLock.Unlock()
	mq.bgLock.Lock()
	defer mq.bgLock.Unlock()

	if mq.closed {
		mq.logger.Warn("mqclient was closed more than once, ignoring")
		return nil
	}

	// Mark the client as closed, we only get one shot to close the
	// Kafka producer and consumer clients.
	mq.closed = true

	// Closing the producer cannot fail, so we do it first
	mq.logger.Info("mqclient closing marker Producer")
	mq.markerProducer.Close()

	if err := mq.consumerClose(); err != nil {
		return err
	}

	return nil
}

func (mq *mqclient) getRedeliveryTimeout() time.Duration {
	return mq.config.RedeliveryTimeout
}

func (mq *mqclient) consumerClose() error {
	mq.logger.Info("mqclient closing Kafka Consumer")
	err := mq.kConsumer.Close()
	if err != nil {
		return errors.Wrapf(err, "mqclient failed to close kConsumer")
	}
	return nil
}

func (mq *mqclient) consumerReadMessage(timeout time.Duration) (*confluentKafka.Message, error) {
	msg, err := mq.kConsumer.ReadMessage(timeout)
	if err != nil {
		if IsReconnectRequired(err) {
			mq.needsReconnect = true
		}
		return nil, errors.Wrapf(err, "mqclient for queue %s failed to read message", mq.queueID)
	}

	return msg, nil
}

func (mq *mqclient) consumerCommitSync(msg *confluentKafka.Message) error {
	offsets, err := mq.kConsumer.Commit()
	if err != nil {
		if IsReconnectRequired(err) {
			mq.needsReconnect = true
		}
		return errors.Wrapf(err, "mqclient for queue %s failed to commit message offsets", mq.queueID)
	}
	mq.logger.Debug("mqclient offset commit success", "offsets", offsets)
	return nil
}

func (mq *mqclient) errClosed() error {
	return errors.Errorf("mqclient for queue %s has been closed", mq.queueID)
}

func newKafkaConsumer(clients kafka.ClientFactory, config MqClientConfig, queueID string) (kafka.Consumer, error) {
	cfg := mqKafkaConsumerConfig(config.BootstrapServers, mqKafkaConsumerGroupID(queueID))
	kc, err := clients.NewConsumer(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "mqclient for queue %s failed to create new kafka consumer with config %+v", queueID, cfg)
	}

	return kc, nil
}

func mqKafkaConsumerGroupID(queueID string) string {
	return "kafka-mq-" + queueID
}

func mqKafkaConsumerConfig(bootstrapServers string, consumerGroupID string) confluentKafka.ConfigMap {
	return confluentKafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           consumerGroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}
}
