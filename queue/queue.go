package queue

import (
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/pkg/logging"
)

type kafkaQueue struct {
	queueID                       string        // the ID of this queue (used for consumer filtering)
	bootstrapServers              string        // the kafka cluster connection information
	messageTopic                  string        // the name of the topic to use for message transport
	markerTopic                   string        // the name of the topic to use for message markers
	redeliveryTimeout             time.Duration // The period after which messages must be redelivered
	keepAlivePeriod               time.Duration // the period to send keep-alive markers (typically half of the redeliveryTimout)
	consumerReceiveBufferCapacity uint          // the capacity of the receive buffer for consumers of this queue (typically equal to the number of connector workers +/- epsilon)
	consumerNextMessageTimeout    time.Duration // timeout used in the consumer loop to read messages off of Kafka
	consumerMaxMessagesPerCommit  uint          // the maximum number of messages consumed before offsets are committed

	kafkaClientFactory kafka.ClientFactory // factory for creating kafka clients

	logger *logging.Logger
}

// NewQueue allocates a new kafkaQueue instance that can be used to produce and consume messages
func NewQueue(queueID string, config Config, kafkaClientFactory kafka.ClientFactory) Queue {
	logger := logging.WithComponent("Queue").With(logging.QueueID, queueID)

	// Send keep alive markers at half the redelivery timeout
	keepAlivePeriod := config.RedeliveryTimeout / 2.0

	logger.Info("new kafkaQueue", "messagetopic", config.MessageTopic, "markerTopic", config.MarkerTopic, "redeliveryTimeout", config.RedeliveryTimeout,
		"keepAlivePeriod", keepAlivePeriod, "consumerReceiveBufferCapacity", config.ConsumerReceiveBufferCapacity,
		"consumerNextMessageTimeout", config.ConsumerNextMessageTimeout)

	return &kafkaQueue{
		queueID:                       queueID,
		bootstrapServers:              config.BootstrapServers,
		messageTopic:                  config.MessageTopic,
		markerTopic:                   config.MarkerTopic,
		redeliveryTimeout:             config.RedeliveryTimeout,
		keepAlivePeriod:               keepAlivePeriod,
		consumerReceiveBufferCapacity: config.ConsumerReceiveBufferCapacity,
		consumerNextMessageTimeout:    config.ConsumerNextMessageTimeout,
		consumerMaxMessagesPerCommit:  config.ConsumerMaxMessagesPerCommit,
		kafkaClientFactory:            kafkaClientFactory,
		logger:                        logger,
	}
}

// Producer returns a kafka-based queue producer
func (kq *kafkaQueue) Producer() (Producer, error) {
	kq.logger.Info("create producer")

	return newProducer(kq)
}

// Consumer returns a kafka-based queue consumer
func (kq *kafkaQueue) Consumer() (Consumer, error) {
	kq.logger.Info("create consumer")

	c, err := newConsumer(kq)
	if err != nil {
		return nil, err
	}

	c.StartConsumerLoop()
	return c, nil
}
