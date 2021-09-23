package queue

import (
	"sync"
	"time"

	"github.com/splunk/kafka-mq-go/pkg/logging"
	"github.com/splunk/kafka-mq-go/queue/internal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// Implements the Message interface
// Each kafkaMessage instance has a background go routine that periodically keeps the message from being redelivered by
// calling back into the mqclient to indicate that message processing is in progress.
type kafkaMessage struct {
	msg             *kafka.Message // the raw message contents
	mqclient        internal.MqClient
	keepAlivePeriod time.Duration
	stopKeepAliveCh chan struct{}  // when closed, signals the background keep-alive timer to shutdown
	once            sync.Once      // used to ensure that the stopKeepAliveChan cannot be closed more than once
	keepAliveWG     sync.WaitGroup // for synchronizing with shutdown of the aforementioned background timer
	logger          *logging.Logger
}

// newMessage creates a new kafkaMessage with the specified raw message contents
func newMessage(queueID string, mqclient internal.MqClient, keepAlivePeriod time.Duration, msg *kafka.Message) (*kafkaMessage, error) {
	if msg == nil {
		return nil, errors.New("raw kafka message must not be nil")
	}
	return &kafkaMessage{
		msg:             msg,
		mqclient:        mqclient,
		keepAlivePeriod: keepAlivePeriod,
		stopKeepAliveCh: make(chan struct{}),
		once:            sync.Once{},
		keepAliveWG:     sync.WaitGroup{},
		logger:          logging.With(logging.QueueID, queueID, logging.Partition, msg.TopicPartition.Partition, logging.Offset, msg.TopicPartition.Offset),
	}, nil
}

// Payload returns the raw message value
func (m *kafkaMessage) Payload() []byte {
	return m.msg.Value
}

// Ack notifies that message processing has been completed.
func (m *kafkaMessage) Ack() error {
	m.logger.Debug("kafkaMessage.Ack")

	m.stopKeepAliveTimer()

	return m.mqclient.Processed(m.msg)
}

// Nack notifies that message processing has failed.
func (m *kafkaMessage) Nack() error {
	m.logger.Debug("kafkaMessage.Nack")

	m.stopKeepAliveTimer()

	return m.mqclient.Processed(m.msg)
}

// Interrupt is called to indicate that processing of the message has been interrupted
func (m *kafkaMessage) Interrupt() error {
	m.logger.Debug("kafkaMessage.Interrupt")

	m.stopKeepAliveTimer()

	return nil
}

// startKeepAliveTimer starts the periodic message keep-alive loop
func (m *kafkaMessage) startKeepAliveTimer() {
	m.logger.Debug("kafkaMessage.StartKeepAliveTimer")
	m.keepAliveWG.Add(1)
	go m.runKeepAliveTimer()
}

// The periodic message keep-alive loop
func (m *kafkaMessage) runKeepAliveTimer() {
	ticker := time.NewTicker(m.keepAlivePeriod)
	for {
		select {
		case <-ticker.C:
			m.logger.Debug("kafkaMessage keep alive timer expired, marking message in progress")
			if err := m.mqclient.ProcessingInProgress(m.msg); err != nil {
				m.logger.Warn("kafkaMessage failed to mark message in progress, ignoring", logging.ErrorKey, err)
			}
		case <-m.stopKeepAliveCh:
			m.logger.Debug("kafkaMessage stopping keep alive timer")
			ticker.Stop()
			m.keepAliveWG.Done()
			return
		}
	}
}

// Synchronously stop the background keep-alive timer loop
func (m *kafkaMessage) stopKeepAliveTimer() {
	m.logger.Debug("kafkaMessage cancelling KeepAlive timer")

	m.once.Do(func() {
		m.logger.Debug("kafkaMessage closing stopKeepAliveChan")
		close(m.stopKeepAliveCh)
	})

	m.logger.Debug("waiting for KeepAlive timer to stop")
	m.keepAliveWG.Wait()
}
