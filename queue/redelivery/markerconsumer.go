package redelivery

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/kafka"
	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal"

	"github.com/cenkalti/backoff"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// MarkerConsumer encapsulates the ability to consume markers from the markers topic and redeliver messages
// redelivery deadlines have expired
type MarkerConsumer interface {
	// Start the `MarkerConsumer`
	Start()

	// Stop the `MarkerConsumer`, it is unusable after it is stopped
	Stop()
}

// MarkerConsumer data structure
// Note that this data structure is not thread-safe, it is only meant to be accessed and modified by the consumer loop
// background thread.  The interface ensures this, as calling code can only start & stop the background thread.
type markerconsumer struct {
	config              queue.Config            // kafka message queue configuration options (topics, timeouts, retries, etc)
	kConsumer           kafka.Consumer          // kafka consumer for reading messages from the marker topic
	offsetCommitter     OffsetCommitter         // for committing consumer offsets of the partitions being consumed
	consumerLoopRunning int32                   // atomic boolean used to control whether the background go routine should exit
	consumerWG          sync.WaitGroup          // for synchronizing with shutdown of the aforementioned background go routine
	messageSender       internal.MessageSender  // used during redelivery to send messages to the queue topic
	markerProducer      internal.MarkerProducer // used during redelivery to send markers to the marker topic

	// We track the partitions that this consumer has been assigned by listening to Kafka consumer group rebalance events.
	// In confluent-kafka-go these events are delivered using the same background go routine that calls `kConsumer.ReadMessage()`
	// in the consumer loop.  Thus, there is no need for this data structure to be synchronized.
	// If the confluent-kafka-go library is upgraded, the library code must be inspected to see whether rebalance events
	// are delivered using the same mechanism, or via a separate go routine.
	assignedPartitions map[int32]AssignedPartition
	logger             *logging.Logger
}

// NewMarkerConsumer constructs a new `MarkerConsumer` instance
func NewMarkerConsumer(config queue.Config, clients kafka.ClientFactory) (MarkerConsumer, error) {
	logger := logging.With(logging.ComponentKey, "markerconsumer")
	logger.Info("creating new marker consumer", "config", fmt.Sprintf("%+v", config))

	messageSender, err := internal.NewMessageSender(config.BootstrapServers, config.MessageTopic, clients)
	if err != nil {
		return nil, err
	}

	mpCfg := internal.MarkerProducerConfig{
		BootstrapServers: config.BootstrapServers,
		Topic:            config.MarkerTopic,
	}
	markerProducer, err := internal.NewMarkerProducer(clients, mpCfg)
	if err != nil {
		return nil, err
	}

	kc, err := newKafkaConsumer(clients, config)
	if err != nil {
		return nil, err
	}

	oc := NewOffsetCommitter(config.MarkerTopic, config.RedeliveryTracker.NumOffsetsPerCommit, kc)

	mc := &markerconsumer{
		config:              config,
		kConsumer:           kc,
		offsetCommitter:     oc,
		consumerWG:          sync.WaitGroup{},
		consumerLoopRunning: 0,
		messageSender:       messageSender,
		markerProducer:      markerProducer,
		assignedPartitions:  make(map[int32]AssignedPartition),
		logger:              logger,
	}

	if err := mc.subscribe(); err != nil {
		return nil, err
	}

	return mc, nil
}

// Start triggers the background thread that cosumes markers from Kafka
func (mc *markerconsumer) Start() {
	if mc.isConsumerLoopRunning() {
		mc.logger.Info("consumer loop is already started")
		return
	}
	mc.logger.Info("starting consumer loop")
	mc.setConsumerLoopRunning(true)
	mc.consumerWG.Add(1)
	go mc.runComsumerLoop()
}

// Stop shuts down the background thread that consumes markers from Kafka.
// It also tears down all resources used by the consumer.  The consumer is unusable after this call.
func (mc *markerconsumer) Stop() {
	mc.logger.Info("stop")

	// Synchonous stop of the consumer loop
	mc.logger.Debug("signalling consumer loop to stop")
	mc.setConsumerLoopRunning(false)

	mc.logger.Info("waiting for consumer loop to stop")
	mc.consumerWG.Wait()

	// Once the consumer has stopped we know that
	// there will be no further calls to Kafka consumer client or offset committer
	mc.logger.Info("closing kafka consumer")
	if err := mc.kConsumer.Close(); err != nil {
		mc.logger.Warn("failed to close kafka consumer, ignoring", logging.ErrorKey, err)
	}

	// Dropping assigned partitions has the side-effect of stopping any re-deliveries that are in-progress
	mc.logger.Info("dropping assigned partitions")
	for partition := range mc.assignedPartitions {
		mc.dropPartition(partition)
	}

	// Once all redeliveries have stopped, there will be no furher attempts to send messages
	// or produce markers
	mc.logger.Info("closing message sender")
	if err := mc.messageSender.Close(); err != nil {
		mc.logger.Warn("failed to close messagesender, ignoring", logging.ErrorKey, err)
	}

	mc.logger.Info("closing marker producer")
	if err := mc.markerProducer.Close(); err != nil {
		mc.logger.Warn("failed to close markerproducer, ignoring", logging.ErrorKey, err)
	}
}

// subscribe uses the kafka consumer to subscribe to the markers topic and listen to consumer group rebalance events
func (mc *markerconsumer) subscribe() error {
	topic := mc.config.MarkerTopic

	mc.logger.Info("subscribing to topic", "topic", topic)
	if err := mc.kConsumer.Subscribe(topic, mc.handleRebalanceEvent); err != nil {
		return errors.Wrapf(err, "marker consumer failed to subscribe consumer to topic %s", mc.config.MarkerTopic)
	}
	return nil
}

// runConsumerLoop is the main message consumer loop
func (mc *markerconsumer) runComsumerLoop() {
	const timeout = 1 * time.Second

	for mc.isConsumerLoopRunning() {
		// Attempt to read a message
		msg := mc.readMessage(timeout)

		now := time.Now()

		// If we've receive a message, add it to our queue and update committed offsets
		if msg != nil {
			mc.handleMessage(msg, now)
		}

		// Regardless of whether we've received a message or not, perform redeliveries if required
		for _, ap := range mc.assignedPartitions {
			ap.Redeliver(now)
		}
	}
	mc.consumerWG.Done()
}

// readMessage attempts to read a message from Kafka.
// `nil` is returned if an error is encountered.
func (mc *markerconsumer) readMessage(timeout time.Duration) *confluentKafka.Message {
	msg, err := mc.kConsumer.ReadMessage(timeout)
	if err != nil {
		if internal.IsReconnectRequired(err) {
			// NOTE: It is not safe to attempt redelivery at this point.  If a reconnect is required,
			//       it may mean that the Kafka brokers could have already rebalanced the consumer group;
			//       the partitions (and offsets for those partitions) that this consumer is tracking
			//       may no longer belong to it.  The safest course of action at this point is to start fresh.
			mc.logger.Fatal(err, "consumer loop failed to read message with fatal err, exiting")
		}
		if internal.IsTimeoutError(err) {
			mc.logger.Debug("consumer loop timed out reading message", logging.ErrorKey, err)
		} else {
			mc.logger.Warn("consumer loop failed to read message", logging.ErrorKey, err)
		}
	}
	return msg
}

// handleMessage attempts to add a message to the relevant AssignedPartition and move the
// committed consumer offset forward if necessary.
func (mc *markerconsumer) handleMessage(msg *confluentKafka.Message, now time.Time) {
	partition := msg.TopicPartition.Partition

	if ap, ok := mc.assignedPartitions[partition]; ok {
		err := ap.AddMessage(msg, now)
		if err != nil {
			mc.logger.Error(err, "failed to add message to assigned partition")
		} else {
			// The assigned partiton has been modified and the smallest offset may have changed.
			// Add the smallest offset to the set of offsets for commit.
			if offset, ok := ap.SmallestMarkerOffset(); ok {
				markerID := NewMarkerID(partition, offset)
				mc.offsetCommitter.AddMarker(markerID)
			}
		}
	} else {
		mc.logger.Warn("read a message for a partition that is not assigned, ignoring", "msg", fmt.Sprintf("%+v", *msg))
	}
}

// handleRebalanceEvent is called when the Kafka consumer group is rebalanced
func (mc *markerconsumer) handleRebalanceEvent(_ *confluentKafka.Consumer, ev confluentKafka.Event) error {
	switch e := ev.(type) {
	case confluentKafka.AssignedPartitions:
		mc.onPartitionsAssigned(e)
	case confluentKafka.RevokedPartitions:
		mc.onPartitionsRevoked(e)
	default:
		// We shouldn't receive other event types here; ignore just to be safe
	}
	return nil
}

// onPartitionsAssigned updates the set of assigned partitons tracked by the marker consumer
func (mc *markerconsumer) onPartitionsAssigned(partitions confluentKafka.AssignedPartitions) {
	mc.logger.Info("onPartitionsAssigned", "assigned", fmt.Sprintf("%+v", partitions))
	for _, p := range partitions.Partitions {
		mc.addPartition(p.Partition)
	}
}

// onPartitionsRevoked updates the set of assigned partitons tracked by the marker consumer
func (mc *markerconsumer) onPartitionsRevoked(partitions confluentKafka.RevokedPartitions) {
	mc.logger.Info("onPartitionsRevoked", "revoked", fmt.Sprintf("%+v", partitions))
	for _, p := range partitions.Partitions {
		mc.dropPartition(p.Partition)
	}
}

// addPartition adds the specified partiton to the set of assigned partitons tracked by the marker consumer
func (mc *markerconsumer) addPartition(partition int32) {
	if _, ok := mc.assignedPartitions[partition]; !ok {
		mc.logger.Info("constructing new assigned partition", partitionLogLabel, partition)

		markersQueue := NewMarkersQueue(partition)
		exponentialBackoff := backoff.NewExponentialBackOff()
		// backoff MaxElapsedTime is a week
		exponentialBackoff.MaxElapsedTime = 168 * time.Hour

		redeliverer := NewRedeliverer(partition, mc.messageSender, mc.markerProducer, exponentialBackoff)
		ap := NewAssignedPartition(markersQueue, redeliverer, mc.config.RedeliveryTracker.UseNowIfNoMarkerSeen)

		mc.assignedPartitions[partition] = ap
	} else {
		mc.logger.Info("assigned partition already exists", partitionLogLabel, partition)
	}
}

// dropPartiton drops the specified partiton from the set of assigned partitons tracked by the marker consumer
func (mc *markerconsumer) dropPartition(partition int32) {
	ap, ok := mc.assignedPartitions[partition]
	if !ok {
		mc.logger.Info("assigned partition already dropped", partitionLogLabel, partition)
		return
	}

	// Release resources associated with the assigned partiton and drop it from the map
	mc.logger.Info("dropping assigned partition", partitionLogLabel, partition)
	if ap != nil {
		ap.Close()
	}
	delete(mc.assignedPartitions, partition)

	mc.logger.Info("dropping cached offsets", partitionLogLabel, partition)
	if mc.offsetCommitter != nil {
		mc.offsetCommitter.DropOffsets(partition)
	}
}

// Atomic check if the consumer loop should be running.
func (mc *markerconsumer) isConsumerLoopRunning() bool {
	return atomic.LoadInt32(&mc.consumerLoopRunning) != 0
}

// Atomic set whether the consumer loop should be running.
func (mc *markerconsumer) setConsumerLoopRunning(running bool) {
	if running {
		atomic.StoreInt32(&mc.consumerLoopRunning, 1)
	} else {
		atomic.StoreInt32(&mc.consumerLoopRunning, 0)
	}
}

func newKafkaConsumer(clients kafka.ClientFactory, config queue.Config) (kafka.Consumer, error) {
	cfg := redeliveryKafkaConsumerConfig(config.BootstrapServers, redeliveryKafkaConsumerGroupID())
	kc, err := clients.NewConsumer(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "marker consumer failed to create new kafka consumer with config %+v", cfg)
	}

	return kc, nil
}

func redeliveryKafkaConsumerGroupID() string {
	return "kafka-mq-redelivery"
}

func redeliveryKafkaConsumerConfig(bootstrapServers string, consumerGroupID string) confluentKafka.ConfigMap {
	return confluentKafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           consumerGroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}
}
