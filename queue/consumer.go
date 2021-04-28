package queue

import (
	"context"
	"sync"
	"sync/atomic"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal"

	"github.com/pkg/errors"
)

// consumer implements the Consumer interface.
// Each consumer instance has a background go routine that consumes messages off of the queue
// and appends them to a channel where they can be received by client calls to `ReceiveMessage()`
type consumer struct {
	*kafkaQueue                               // the associated queue
	mqclient                internal.MqClient // a client to pull messages from the queue
	receiveCh               chan Message      // messages are consumed by a background go routine and appended to this channel where they can be received
	onceCloseReceiveCh      sync.Once         // used to ensure that `receiveCh` can only be closed once
	stopConsumerCh          chan struct{}     // when closed, signals the background consumer loop to exit
	onceCloseStopConsumerCh sync.Once         // used to ensure that the `stopConsumerCh` can only be closed once
	consumerWG              sync.WaitGroup    // for synchronizing with shutdown of the aforementioned background go routine
	consumerLoopRunning     int32             // atomic boolean used to control whether the background go routine should exit
	logger                  *logging.Logger
}

// newConsumer creates a consumer instance for the specified queue
func newConsumer(kq *kafkaQueue) (*consumer, error) {
	if kq == nil {
		return nil, errors.New("kafkaQueue must not be nil")
	}

	logger := logging.WithComponent("Consumer").With(logging.QueueID, kq.queueID)
	logger.Info("consumer creating mqclient")

	mqConfig := internal.MqClientConfig{
		BootstrapServers:     kq.bootstrapServers,
		MessageTopic:         kq.messageTopic,
		MarkerTopic:          kq.markerTopic,
		RedeliveryTimeout:    kq.redeliveryTimeout,
		MaxMessagesPerCommit: kq.consumerMaxMessagesPerCommit,
	}
	mq, err := internal.NewMqClient(
		kq.kafkaClientFactory,
		mqConfig,
		kq.queueID,
	)
	if err != nil {
		return nil, err
	}

	return &consumer{
		kafkaQueue:              kq,
		mqclient:                mq,
		receiveCh:               make(chan Message, kq.consumerReceiveBufferCapacity),
		onceCloseReceiveCh:      sync.Once{},
		stopConsumerCh:          make(chan struct{}),
		onceCloseStopConsumerCh: sync.Once{},
		consumerWG:              sync.WaitGroup{},
		consumerLoopRunning:     1,
		logger:                  logger,
	}, nil
}

func (c *consumer) Receive(ctx context.Context) (Message, error) {
	select {
	case m, ok := <-c.receiveCh:
		if !ok {
			return nil, errors.New("consumer.ReceiveMessage was called after the consumer was closed")
		}
		return m, nil
	case <-ctx.Done():
		return nil, nil
	}
}

func (c *consumer) Close() error {
	c.logger.Info("consumer.Close")

	c.logger.Debug("signalling consumer loop to stop")
	c.setConsumerLoopRunning(false)

	c.onceCloseStopConsumerCh.Do(func() {
		c.logger.Info("closing stopConsumerLoopCh")
		close(c.stopConsumerCh)
	})

	c.logger.Info("waiting for consumer loop to stop")
	c.consumerWG.Wait()

	c.onceCloseReceiveCh.Do(func() {
		c.logger.Info("closing receiveChan")
		close(c.receiveCh)
	})

	c.logger.Info("closing mqclient")
	return c.mqclient.Close()
}

// StartConsumerLoop starts the background thread that reads data from kafkad
// NOTE: this method is exported for unit test purposes
func (c *consumer) StartConsumerLoop() {
	c.logger.Info("consumer.StartConsumerLoop")
	c.consumerWG.Add(1)
	go c.runComsumerLoop()
}

func (c *consumer) reconnectMqClientIfNeeded() {
	// Check if this error requires recovery action
	// It is important to check if a reconnect is needed before checking if a reset is needed, because
	// a client reset will fail if a reconnect is needed.
	if c.mqclient.NeedsReconnect() {
		c.logger.Info("consumer mqclient is disconnected, attempting to reconnect")
		if err := c.mqclient.Reconnect(); err != nil {
			c.logger.Warn("consumer failed to reconnect mqclient", logging.ErrorKey, err)
		}
	}
}

func (c *consumer) resetMqClientIfNeeded() {
	if c.mqclient.NeedsReset() {
		c.logger.Info("consumer mqclient is tainted, attempting to reset")
		if err := c.mqclient.Reset(); err != nil {
			c.logger.Warn("consumer failed to reset mqclient", logging.ErrorKey, err)
		}
	}
}

// The main message consumer loop.
// Pulls messages off of the queue using the mqclient and appends them to the receive channel.
func (c *consumer) runComsumerLoop() {
	const (
		msgLabel = "msg"
	)
	for c.isConsumerLoopRunning() {
		if raw, err := c.mqclient.NextMessage(c.consumerNextMessageTimeout); err != nil {
			if internal.IsTimeoutError(err) {
				c.logger.Debug("consumer timed out getting next message from mqclient", logging.ErrorKey, err)
			} else {
				c.logger.Warn("consumer failed to get next message from mqclient", logging.ErrorKey, err)
			}
			c.reconnectMqClientIfNeeded()
			c.resetMqClientIfNeeded()
		} else {
			if msg, err := newMessage(c.queueID, c.mqclient, c.keepAlivePeriod, raw); err != nil {
				c.logger.Warn("consumer failed construct message wrapper", msgLabel, msg, logging.ErrorKey, err)
			} else {
				// Start the keep alive timer for the msg and attempt to enqueue it on the receive channel.
				// The select loop blocks until there is space in the receive channel buffer or stop signal is received.
				msg.startKeepAliveTimer()
				select {
				case c.receiveCh <- msg:
					// message enqueued on the receive channel
					c.logger.Debug("consumer enqueued on the receive channel", msgLabel, msg)
				case <-c.stopConsumerCh:
					c.logger.Info("consumer received stop signal")
				}
			}
		}
	}
	c.consumerWG.Done()
}

func (c *consumer) isConsumerLoopRunning() bool {
	return atomic.LoadInt32(&c.consumerLoopRunning) != 0
}

func (c *consumer) setConsumerLoopRunning(running bool) {
	if running {
		atomic.StoreInt32(&c.consumerLoopRunning, 1)
	} else {
		atomic.StoreInt32(&c.consumerLoopRunning, 0)
	}
}
