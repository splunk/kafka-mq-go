package internal

import (
	"time"

	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/pkg/logging"
	kafkamq "github.com/splunk/kafka-mq-go/queue/proto"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// MarkerProducer is an interface that supports the sending marker messages
// to the `MarkersTopic` in support of the kafka-based message queue protocol
type MarkerProducer interface {
	// SendStartMarker synchronously sends a start marker for the specified kafka message and redelivery timeout to the markers topic.
	// This marks the beginning of processing for the message and sets the redelivery deadline.
	SendStartMarker(queueID string, msg *confluentKafka.Message, redeliveryTimeout time.Duration) error

	// SendKeepAliveMarker synchronously sends a keep-alive marker for the specified kafka message and redelivery timeout to the markers topic.
	// This marks processing of the message as still in progress and refreshes the redelivery deadline.
	SendKeepAliveMarker(queueID string, msg *confluentKafka.Message, redeliveryTimeout time.Duration) error

	// SendEndMarker synchronously sends a end marker for the specified kafka message to the markers topic.
	// This marks the end of processing for the message.
	SendEndMarker(queueID string, msg *confluentKafka.Message) error

	// Close tears down the resources associated with this marker producer; it is unusable after this call.
	Close() error
}

// MarkerProducerConfig is a bundle of configuration for a `MarkerProducer` instance
type MarkerProducerConfig struct {
	BootstrapServers string // kafka cluster connection information
	Topic            string // the name of the topic that stores markers
}

// makerproducer is an implementation of the MarkerProducer interface
// The implementation is thread-safe as the underlying `KafkaProducer` is thread-safe
type markerproducer struct {
	config    MarkerProducerConfig
	kProducer kafka.Producer // kafka producer for writing markers to the markers topic
	logger    *logging.Logger
}

// NewMarkerProducer returns a handle to an open `MarkerProducer` instance.
func NewMarkerProducer(clients kafka.ClientFactory, config MarkerProducerConfig) (MarkerProducer, error) {
	logger := logging.WithComponent("MarkerProducer")
	logger.Info("new markerproducer", "config", config)

	kpCfg := mpKafkaProducerConfig(config.BootstrapServers)
	kp, err := clients.NewProducer(kpCfg)
	if err != nil {
		return nil, errors.Wrapf(err, "markerproducer failed to create new kafka producer with config %+v", kpCfg)

	}
	return &markerproducer{
		config:    config,
		kProducer: kp,
		logger:    logger,
	}, nil
}

func (mp *markerproducer) SendStartMarker(queueID string, msg *confluentKafka.Message, redeliveryTimeout time.Duration) error {
	mp.logger.Debug("markerproducer SendStartMarker")
	m := startMarker(msg, redeliveryTimeout)
	return mp.sendMarkerSync(queueID, m)
}

func (mp *markerproducer) SendKeepAliveMarker(queueID string, msg *confluentKafka.Message, redeliveryTimeout time.Duration) error {
	mp.logger.Debug("markerproducer SendKeepAliveMarker")
	m := keepAliveMarker(msg, redeliveryTimeout)
	return mp.sendMarkerSync(queueID, m)
}

func (mp *markerproducer) SendEndMarker(queueID string, msg *confluentKafka.Message) error {
	mp.logger.Debug("markerproducer SendEndMarker")
	m := endMarker(msg)
	return mp.sendMarkerSync(queueID, m)
}

func (mp *markerproducer) GetTopic() string {
	return mp.config.Topic
}

func (mp *markerproducer) Close() error {
	mp.logger.Info("markerproducer closing kafka Producer")
	mp.kProducer.Close()
	return nil
}

// sendMarkerSync serializes the specified marker, sends it to the marker topic, and does not return until delivery has completed.
func (mp *markerproducer) sendMarkerSync(queueID string, marker kafkamq.Marker) error {
	deliveryChan := make(chan confluentKafka.Event)
	defer close(deliveryChan)

	topicPartition := confluentKafka.TopicPartition{Topic: &mp.config.Topic, Partition: confluentKafka.PartitionAny}

	mp.logger.Debug("markerproducer attempting to produce marker", "marker", marker, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)

	data, err := marker.Marshal()
	if err != nil {
		return errors.Wrapf(err, "markerproducer failed to marshal marker message: %+v", marker)
	}

	msg := &confluentKafka.Message{
		TopicPartition: topicPartition,
		Key:            []byte(queueID),
		Value:          data,
	}
	if err := mp.kProducer.Produce(msg, deliveryChan); err != nil {
		return errors.Wrapf(err, "mqclient failed to produce marker for queue %s on topic: %+v", queueID, topicPartition)
	}

	mp.logger.Debug("mqclient waiting for marker delivery to complete", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)
	e := <-deliveryChan

	// Check whether delivery succeeded
	m, ok := e.(*confluentKafka.Message)
	if !ok {
		return errors.Errorf("mqclient received a delivery event that is not of the expected type *Message, got: %T", e)
	}

	if m.TopicPartition.Error != nil {
		return errors.Wrapf(m.TopicPartition.Error, "mqclient marker delivery failed for queue %s on topic: %+v", queueID, topicPartition)
	}

	mp.logger.Debug("mqclient delivered marker", logging.QueueID, queueID, logging.Topic, topicPartition.Topic, logging.Partition, topicPartition.Partition)

	return nil
}

func startMarker(msg *confluentKafka.Message, redeliveryTimeout time.Duration) kafkamq.Marker {
	m := newMarker(kafkamq.Marker_START, msg)
	m.Key = msg.Key
	m.Value = msg.Value
	m.RedeliverAfterMs = durationToMs(redeliveryTimeout)
	return m
}

func endMarker(msg *confluentKafka.Message) kafkamq.Marker {
	return newMarker(kafkamq.Marker_END, msg)
}

func keepAliveMarker(msg *confluentKafka.Message, redeliveryTimeout time.Duration) kafkamq.Marker {
	m := newMarker(kafkamq.Marker_KEEPALIVE, msg)
	m.RedeliverAfterMs = durationToMs(redeliveryTimeout)
	return m
}

func newMarker(markerType kafkamq.Marker_Type, msg *confluentKafka.Message) kafkamq.Marker {
	return kafkamq.Marker{
		Version: kafkamq.Marker_V1,
		Type:    markerType,
		MessageId: &kafkamq.MessageID{
			Partition: msg.TopicPartition.Partition,
			Offset:    int64(msg.TopicPartition.Offset),
		},
	}
}

func durationToMs(d time.Duration) uint64 {
	return uint64(d / time.Millisecond)
}

func mpKafkaProducerConfig(bootstrapServers string) confluentKafka.ConfigMap {
	return confluentKafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",            // wait for all acknowledgements before delivery is complete
		"retries":           0,                // do not allow out-of-order message deliveries (send markers must arrive before their associated end markers)
		"linger.ms":         1,                // give some time for messages to be batched
		"partitioner":       "murmur2_random", // Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned).
	}
}
