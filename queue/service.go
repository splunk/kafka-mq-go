package queue

import (
	"github.com/splunk/kafka-mq-go/pkg/kafka"
	"github.com/splunk/kafka-mq-go/pkg/logging"
)

// service implements Service interface
type service struct {
	kafkaClientFactory kafka.ClientFactory
	config             Config
	logger             *logging.Logger
}

// NewService returns a Service instance backed by Kafka
func NewService(kafkaClientFactory kafka.ClientFactory, config Config) (Service, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &service{
		config:             config,
		kafkaClientFactory: kafkaClientFactory,
		logger:             logging.WithComponent("Service"),
	}, nil
}

// GetOrCreateQueue returns a `Queue` identified by the specified identifier.
func (s *service) GetOrCreateQueue(id string) Queue {
	s.logger.Info("create kafka queue", logging.QueueID, id)

	return s.createQueueInternal(id)
}

// createQueueInternal is a helper method to allocate a new queue instance for a queue with the specified id
func (s *service) createQueueInternal(id string) Queue {
	return NewQueue(id, s.config, s.kafkaClientFactory)
}
