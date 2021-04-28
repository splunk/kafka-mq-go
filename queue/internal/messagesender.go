package internal

import (
	"context"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka"
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// MessageSender supports sending a message to a specific kafka-based queue
// It is used by both the `queue.Producer` and `redelivery.Redeliverer` implementations
type MessageSender interface {
	// SendMessage sends a message containing `payload` to the kafka-based queue with name `queueID`
	SendMessage(queueID string, payload []byte) error

	// SendMessageAsync sends a message asynchronously containing `payload` to the kafka-based queue with name `queueID`
	SendMessageAsync(queueID string, payload []byte, deliveryCh chan kafka.Event)

	// Flush flushes all pending messages
	Flush(ctx context.Context)

	// Close tears down the resources associated with this sender; it is unusable after this call.
	Close() error
}

type messagesender struct {
	topic     string
	kProducer kafka.Producer
	logger    *logging.Logger
}

// NewMessageSender creates a new `Sender` instance that is capable of sending kafka-based queue messages
// to the specified topic.
func NewMessageSender(bootstrapServers string, topic string, kafkaClientFactory kafka.ClientFactory) (MessageSender, error) {
	logger := logging.WithComponent("MessageSender").With("bootstrapServers", bootstrapServers, logging.Topic, topic)
	logger.Info("producer creating kafka producer")

	cfg := kafkaProducerConfig(bootstrapServers)
	kp, err := kafkaClientFactory.NewProducer(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create kafka producer with config: %+v", cfg)
	}

	return &messagesender{
		topic:     topic,
		kProducer: kp,
		logger:    logger,
	}, nil
}

func (s *messagesender) SendMessage(queueID string, payload []byte) error {
	deliveryChan := make(chan confluentKafka.Event)
	defer close(deliveryChan)

	topicPartition := confluentKafka.TopicPartition{Topic: &s.topic, Partition: confluentKafka.PartitionAny}

	s.logger.Debug("attempting to produce message", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)

	err := s.kProducer.Produce(&confluentKafka.Message{
		TopicPartition: topicPartition,
		Key:            []byte(queueID),
		Value:          payload,
	}, deliveryChan)

	if err != nil {
		return errors.Wrapf(err, "failed to produce message for queue %s on topic: %+v", queueID, topicPartition)
	}

	s.logger.Debug("waiting for message delivery to complete", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)
	e := <-deliveryChan

	// Check whether delivery succeeded
	m, ok := e.(*confluentKafka.Message)
	if !ok {
		return errors.Errorf("received a delivery event that is not of the expected type *Message, got: %T", e)
	}

	if m.TopicPartition.Error != nil {
		return errors.Wrapf(m.TopicPartition.Error, "message delivery failed for queue %s on topic: %+v", queueID, topicPartition)
	}

	s.logger.Debug("delivered message", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)

	return nil
}

func (s *messagesender) SendMessageAsync(queueID string, payload []byte, deliveryCh chan kafka.Event) {
	topicPartition := confluentKafka.TopicPartition{Topic: &s.topic, Partition: confluentKafka.PartitionAny}

	s.logger.Debug("attempting to produce message", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)

	err := s.kProducer.Produce(&confluentKafka.Message{
		TopicPartition: topicPartition,
		Key:            []byte(queueID),
		Value:          payload,
	}, deliveryCh)
	if err != nil {
		deliveryCh <- err.(kafka.ErrorEvent)
	}
}

func (s *messagesender) Flush(ctx context.Context) {
	s.logger.Info("sender.Flush")
	timeoutMs := 10
	remaining := s.kProducer.Flush(timeoutMs)
	for remaining > 0 {
		select {
		case <-ctx.Done():
			return
		default:
		}
		remaining = s.kProducer.Flush(timeoutMs)
	}
}

func (s *messagesender) Close() error {
	s.logger.Info("sender.Close")
	s.kProducer.Close()
	return nil
}

func kafkaProducerConfig(bootstrapServers string) confluentKafka.ConfigMap {
	return confluentKafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"partitioner":       "random", // messages for all queues should be evenly distributed across all partitions
		// "compression.type":  "zstd",
	}
}
