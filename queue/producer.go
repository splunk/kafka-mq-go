package queue

import (
	"context"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal"

	"github.com/pkg/errors"
)

// producer implements the Producer interface
type producer struct {
	*kafkaQueue
	messageSender internal.MessageSender
	logger        *logging.Logger
}

func newProducer(kq *kafkaQueue) (*producer, error) {
	if kq == nil {
		return nil, errors.New("kafkaQueue must not be nil")
	}

	logger := logging.WithComponent("Producer").With(logging.QueueID, kq.queueID)
	logger.Info("producer creating sender")

	sender, err := internal.NewMessageSender(kq.bootstrapServers, kq.messageTopic, kq.kafkaClientFactory)
	if err != nil {
		return nil, err
	}

	return &producer{
		kafkaQueue:    kq,
		messageSender: sender,
		logger:        logger,
	}, nil
}

func (p *producer) Send(payload []byte) error {
	return p.messageSender.SendMessage(p.queueID, payload)
}

func (p *producer) SendAsync(payload []byte, deliveryCh chan Event) {
	p.messageSender.SendMessageAsync(p.queueID, payload, deliveryCh)
}

func (p *producer) Flush(ctx context.Context) {
	p.logger.Info("producer.Flush")
	p.messageSender.Flush(ctx)
}

func (p *producer) Close() error {
	p.logger.Info("producer.Close")
	p.messageSender.Close()
	return nil
}
