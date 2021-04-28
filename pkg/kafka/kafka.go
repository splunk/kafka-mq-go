package kafka

import (
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	configKey = "Config"
)

// Event is a type alias for confluent-kafka-go's delivery event type
// returned on the `deliveryChan` provided to the Producer's Produce() method
type Event = kafka.Event

// MessageDeliveredEvent is a type alias for confluent-kafka-go's message delivered notification
// returned on the `deliveryChan` provided to the Producer's Produce() method
type MessageDeliveredEvent = kafka.Message

// ErrorEvent is a type alias for confluent-kafka-go's message delivery error notification
// returned on the `deliveryChan` provided to the Producer's Produce() method
type ErrorEvent = kafka.Error

// Producer allows clients to send messages to Kafka
// NB: this interface exists for mocking purposes
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timoutMs int) int
	Close()
}

// Consumer allows clients to read messages from Kafka
// NB: this interface exists for mocking purposes
type Consumer interface {
	Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	Commit() ([]kafka.TopicPartition, error)
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Close() error
}

// ClientFactory provides methods for creating new Producer and Consumer instances
type ClientFactory interface {
	// NewProducer returns a new Producer instance configured with the specified `config`
	NewProducer(config kafka.ConfigMap) (Producer, error)

	// NewConsumer returns a new Consumer instance configured with the specified `config`
	NewConsumer(config kafka.ConfigMap) (Consumer, error)
}

type kafkaClientFactory struct {
	logger *logging.Logger
}

// NewClientFactory returns a new ClientFactory instance
func NewClientFactory() ClientFactory {
	return &kafkaClientFactory{
		logger: logging.WithComponent("KafkaClientFactory"),
	}
}

// NewKafkaProducer returns a new Producer instance configured with the specified `config`
func (f *kafkaClientFactory) NewProducer(config kafka.ConfigMap) (Producer, error) {
	f.logger.Info("creating kafka producer client", configKey, config)
	return kafka.NewProducer(&config)
}

// NewKafkaConsumer returns a new Consumer instance configured with the specified `config`
func (f *kafkaClientFactory) NewConsumer(config kafka.ConfigMap) (Consumer, error) {
	f.logger.Info("creating kafka consumer client", configKey, config)
	return kafka.NewConsumer(&config)
}
