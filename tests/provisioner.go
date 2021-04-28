package tests

import (
	"context"

	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

const (
	numMessageTopicPartitions = 2
	numMarkerTopicPartitions  = 2
	topicReplicationFactor    = 1
)

// Provisioner encapsulates provisioning the test environment
type Provisioner struct {
	conf  queue.Config
	admin *kafka.AdminClient
}

// NewProvisioner creates a new provisioner instance
func NewProvisioner(conf queue.Config) (*Provisioner, error) {
	kafkaConf := kafka.ConfigMap{
		"bootstrap.servers": conf.BootstrapServers,
	}
	admin, err := kafka.NewAdminClient(&kafkaConf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka admin client")
	}
	return &Provisioner{
		conf:  conf,
		admin: admin,
	}, nil
}

// ProvisionTopics creates the Kafka topics for (re)delivering queue messages
func (p *Provisioner) ProvisionTopics(ctx context.Context) error {
	specs := []kafka.TopicSpecification{
		kafka.TopicSpecification{
			Topic:             p.conf.MessageTopic,
			NumPartitions:     numMessageTopicPartitions,
			ReplicationFactor: topicReplicationFactor,
		},
		kafka.TopicSpecification{
			Topic:             p.conf.MarkerTopic,
			NumPartitions:     numMarkerTopicPartitions,
			ReplicationFactor: topicReplicationFactor,
		},
	}
	if _, err := p.admin.CreateTopics(ctx, specs); err != nil {
		return errors.Wrap(err, "failed to create topics")
	}
	return nil
}

// TeardownTopics deletes the marker and message topics from Kafka
func (p *Provisioner) TeardownTopics(ctx context.Context) error {
	topics := []string{
		p.conf.MessageTopic,
		p.conf.MarkerTopic,
	}
	if _, err := p.admin.DeleteTopics(ctx, topics); err != nil {
		return errors.Wrap(err, "failed to delete topics")
	}
	return nil
}
