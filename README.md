# kafka-mq-go

kafka-mq-go provides message queue delivery and processing semantics on top of the Kafka streaming service.

Kafka allows for all messages up to a specific offset to be acknowledged whereas kafka-mq-go allows individual messages to be acknowledged in the order that they are processed, which may be different from the order that they were read from the underlying stream.

# Design

Multiple logical message queues are built on top of a pair of physical Kafka topics:
* The *queue topic* contains messages produced by `queue.Producer` instances.  Each message in the queue topic is keyed by the unique identifier of the queue that it belongs to.
    * Messages are consumed from the queue topic by `queue.Consumer` instances.  As consumers process messages they record their progress by sending marker messages to the *markers topic*
* The *markers topic* contains a log that tracks message processing progress for all queues in the system.
    * Marker messages are consumed by a number of `RedeliveryTracker` replicas that determine when messages need to be redelivered to the queue topic.

![alt text][design]

[design]: ./design.png "Kafka-based message queue"

## Producer Operation

Message production performed by `queue.Producer` instances is denoted in step 0 in the diagram above; producer records with the following properties will be sent to the *queue topic*:

* `key = queue_id`
* `value = message_payload`

Records are keyed with the unique identifier of the queue that they are associated with to allow for efficient filtering of messages by `queue.Consumer` instances.  In order to ensure that all messages are evenly distributed across queue topic partitions, the `random` partitioner is used by the Kafka Producer instances contained in each `queue.Producer`.

## Consumer Operation

Message consumption and processing is performed in steps 1-5 in the diagram above.  When a `queue.Consumer` starts up it subscribes to the queue topic with a consumer group that is associated with the logical queue that the `queue.Consumer` will be consuming messages from.  Processing of messages by the consumer is performed as follows:

1. A batch of messages is consumed from the queue topic.
    * Messages are filtered to the queue that the `queue.Consumer` is processing messages from using the Kafka ConsumerRecord key.
2. A *start marker* message is produced on the markers topic to denote that message processing has started.
    * All marker messages will have the following properties:
        * `type : Enum(Start, End, KeepAlive)`
            * `Start` - signifies that processing of a message has started; begin tracking the message for redelivery.
            * `End` - signifies that processing of a message has ended; stop tracking the message for redelivery.
            * `KeepAlive` - signifies that processing of a message is in progress; continue tracking the message for redelivery.
        * `partition : Int` - the ordinal of the partition in the source topic that contains the message.
        * `offset : Long` - the offset in the partition where the message is located.
        * `redeliver_after_ms: Long` - the number of milliseconds to wait between when this marker is seen by the `RedeliveryTracker` and when the associated message should be redelivered.  Only set for `Start` and `KeepAlive` markers.
        * `key : bytes` - the key of the Kafka ConsumerRecord in the source topic that contains the message.  Sent so that the message can be redelivered to the correct logical queue.
        * `value : bytes` - the value of the Kafka ConsumerRecord in the source topic that contains the message.  Sent so that the original message contents can be redelivered.  Only set for `Start` messages.
    * In an effort to conserve storage:
        * `Start` markers will have all fields set.
        * `KeepAlive` markers will not have key and value fields set.
        * `End` markers will not have redeliver_after_ms, key, and value fields set.
3. The consumer offset is committed to the queue topic in Kafka.
4. The `queue.Consumer` processes the message and starts a background go-routine that periodically produces a `KeepAlive` marker message on the markers topic.
5. When message processing is complete the `queue.Consumer`:
    * Cancels the associated `KeepAlive` marker go-routine.
    * Produces an `End` marker message on the markers topic to record that task processing has finished.

### Producing Marker Messages

The markers topic contains multiple partitions; for correctness it is important that all markers for a message are processed by the `RedeliveryTracker` in the order that they are produced by the `queue.Consumer`.  Thus, we ensure that all markers for a particular logical queue are written to the same partition in the markers topic.  This is acheived by using the default Kafka partitioner and setting the key for Kafka ProducerRecord that contains the marker payload as follows: `key = queue_id`.

### Consumer Failure & Restart

The `queue.Consumer` has a consumer group id based on the unique identifier of the queue that is consuming; this ensures that each `queue.Consumer` sees every message that flows through the queue topic.

The `queue.Consumer` does not maintain local state; on restart after failure it will continue consuming from the the last committed offset and the messages whose processing were in-progress at the time of the failure will be re-delivered to the queue topic (see Redelivery Operation below).

## Redelivery Operation

The `RedeliveryTracker` component ensures that messages whose processing were interrupted due to the crash of a `queue.Consumer` are re-delivered to the queue topic; this allows for message processing to be retried.  The `RedeliveryTracker` consumes from the markers topic in order to determine which messages require redelivery.  A number of `RedeliveryTracker` replicas can be deployed to share consumption of the partitions in the markers topic in order to scale out the capacity for redelivery.

When a `RedeliveryTracker` subscribes to the markers topic it is assigned a set of partitions.  For each partition, the RedeliveryTracker maintains the following in-memory data structures:

* `markersInProgress` - a map from message identity (partition & offset in the queue topic) to marker data (key, value) to track message that are currently being processed.
* `markersByOffset` - a priority queue of message identity & marker_offset ordered by the marker_offset.  Used to track the RedeliveryTracker's consumer offset in the partition.
* `markersByTimestamp` - a priority queue of message identity & redelivery_deadline (marker_timestamp + messgage_expiry_time) ordered by redelivery_deadline.  Used to determine when messages require redelivery to the queue topic.

Message redelivery is performed in two parts: consumption of message markers and message redelivery.  Consumption of markers is denoted with dashed arrows in the diagram above.

1. The RedeliveryTracker consumes markers from the markers topic.
2. The RedeliveryTracker deserializes each marker, determines which markers topic partition it is associated with, and updates the in-memory data-structures as follows depending on the type of marker message:
    * Start
        1. A mapping is created to store the redelivery data for the message in `markersInProgress` `(partition,offset) => (key, data)`.
        2. marker entry `(partition,offset,marker_offset)` is inserted into the `markersByOffset` queue.
        3. marker entry `(partition,offset,redelivery_deadline)` is inserted into the `markersByTimestamp` queue.
    * End
        1. marker entry is removed from the `markersByOffset` queue.
        2. marker entry is removed from the `markersByTimestamp` queue.
        3. mapping is removed from markers in `markersInProgress`.
    * KeepAlive
        1. marker entry's priority is adjusted in the `markersByTimestamp` queue according to updated redelivery_deadline.
3. The RedeliveryTracker determines the smallest start marker offset of messages that are in progress by querying the `markersByOffset` priority queue and committing this offset explicitly.

Redelivery of expired messages is denoted with dotted arrows in the diagram above.

1. The `RedeliveryTracker` consumer go-routine periodically checks its `markersByTimestamp` queues for messages whose execution deadline has expired.
2. For messages that have expired, the content of the `markersInProgress` map (message key, value) is used to redeliver the message payload to the queue topic.
3. Once redelivery is complete, the marker entries for the task will be removed from both `markersByTimestamp` and `markersByOffset` queues, as well as the `markersInProgress` map.

### RedliveryTracker Failure & Restart

The RedeliveryTracker needs to rebuild the data structures for tracking in-progress messages when recovering from a crash.  By committing the offset of the smallest start marker in the queue during marker topic consumption we guarantee that the RedeliveryTracker will rebuild the state of the priority queues and `markersInProgress` map from the log at the time that the tracker crashed.

## Prior Art
The design follows a similar approach to that employed by [kmq](https://github.com/softwaremill/kmq) with the following differences:

1. Queue consumers are able to refresh the redelivery deadline for messages while they are being processed.
    * This is important for systems where message processing time is highly variable.  For example, some messages may be processed in seconds whereas others may take hours or days for processing to complete.
    * In kmq, redelivery deadlines are computed once per-message and are immutable.
2. Messages for multiple logical message queues are multiplexed onto the same physical Kafka topics.
    * This is important for systems where the set of queues in the system is dynamic.  For example, job scheduling systems where the set of jobs is configurable at runtime and per-job queues are used to dispatch tasks to workers.
    * In Kafka, topics and their associated partitions are heavy-weight entities.  Historically, Kafka is not  great when handling large numbers of changes to the set of active topics.
    * In kmq, a pair of physical topics are required for each logical queue.
3. Message redelivery does not require the RedeliveryTracker component to [seek()](https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#seek(org.apache.kafka.common.TopicPartition,%20long)) in the queue topic to fetch the contents of messages that require redelivery.
    * This is important as messages for all logical queues flow through the same physical Kafka topic and the throughput requirements for each logical queue may be different.  It is possible for the original message contents of a low-volume logical queue to be deleted due to Kafka topic retention policies before redelivery is attempted.
    * This also means that queue processing throughput is unaffected by message redelivery when it occurs.  Kafka relies on the system page cache (and message recency) for the performance characteristics that it has, and introducing a `seek()` has a negative effect on performance.
    * In addition, avoiding `seek()` calls allows for the physical queue and markers topics to be configured with different numbers of partitions, retention policies, and other configuration, allowing for consumption of message and markers topics to be scaled independently as required.
    * In kmq, the redelivery component performs a `seek()` to fetch the contents of the message that requires redelivery.  In order to reduce the negative impact on queue processsing throughput, kmq ensures that only forward `seek()`s are performed by requiring that the message & markers topics have equal number of partitions.  With this constraint, there will only ever be one redelivery component performing `seek()` operations on particular message topic at any time, minimising the amount of disruptive churn in the page cache.
4. Message redelivery deadlines are immune to clock skew between message consumers, Kafka brokers, and redelivery tracker instances.
    * Similar to kmq, the timestamps of markers are used as the logical deadline clock that determines whether redelivery is required.
    * Unlike kmq, in the absence of new markers the value of the local clock of the redelivery tracker is *not used as an absolute value*.  Instead, a local monotonic clock is used to determine the deadline clock value by taking the difference between the monotoic clock value at the time the last marker was received and the current monotonic clock value.
5. Back-pressure is built into queue message consumers.
    * This is important in order to place a limit on the number of marker messages produced (proportional to the number of messages in-progress) and avoids a possible flood of message re-deliveries.

# Possible Improvements

The following is a list of useful improvements that could be implemented.

1. TTL for message redelivery to limit the number of times that a message will be re-delivered to the queue in the case of message processor failure.
    * This would be equivalent to the [SQS maxReceiveCount](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html) configurable.
    * Approach:
        * Tunnel the TTL value inside queue messages and propagate the value in markers.
        * Decrement the TTL value before re-delivering messages and abort redelivery when the TTL value reaches zero.
2. Instrument with metrics to improve observability.  For example:
    * Counters for messages produced and consumed.
    * Guages for the number of messages being processed and redelivered
    * Histograms to track message production and consumption latency.
    * The [Prometheus Go Client](https://github.com/prometheus/client_golang) is an excellent library for these purposes.
3. Performance improvements.
    * The current implementation performs a few allocations on the heap per-message which brings the Golang GC onto the hot code path.  Stack allocation should be used on hot paths and is preferred in other message processing clients written in Go.  See [this PR from the apache-pulsar-go client for an example](https://github.com/apache/pulsar-client-go/pull/319).
4. Reduce scenarios where message processing may be duplicated.  Currently there are a few places where duplicates can be generated:
    1. `queue.Producer`: the Kafka Producer client sends a message with at-least-once semantics.
        * Producer sends message `M` and waits for delivery acknowledgement.
        * Kafka Broker receives `M`, sends acknowledgement `ACK(M)`, but the acknowledgment is slow to arrive at the Producer.
        * Producer times out waiting for `ACK(M)` and retries by sending message `M` again.
        * Kafka Broker receives duplicate `M`.
    2. `queue.Redeliverer`: the Kafka Producer client used to redeliver messages to the queue has at-least-once semantics.
        * Effectivly the same as scenario 1.
    3. `queue.Consumer`:  Failure to commit offsets after sending a marker will cause redelivery.
        * Consumer reads message `M` from the queue topic and sends start maker `START(M)` to the markers topic.
        * Consumer crashes before it can commit the offset for `M` to the queue topic.
        * Redelivery tracker observes redelivery deadline of `START(M)` expiring, and redelivers `M` to the queue topic as `M'`.
        * Queue topic now contains two messages `M` and `M'` with the same contents.
    * Scenarios 1 & 2: Preventing duplicates when sending messages to the queue topic.
        * In order to avoid introducing duplicates when sending messages to the queue topic the `queue.Producer` implementation could make use of the [Idempotent Kafka Producer](https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer).  The idempotent Producer allows Kafka Brokers to identify duplicate messages introduced by Producer retries.
    * Scenario 3: Preventing duplicates due to queue topic commit offsets and marker topic contents becoming out of sync.
        * The sensitive sequence of operations is:
            1. Consumer reads message `M` from queue topic
            2. Consumer sends a start marker `START(M)` to markers topic
            3. Consumer commits offset for message `M` to queue topic
        * The [Kafka Transaction API](https://www.confluent.io/blog/transactions-apache-kafka/) can be used to wrap operations 2 and 3 in an atomic transaction.
# Requirements

The following are required for building and running tests.
* [Protocol Buffers Compiler](https://developers.google.com/protocol-buffers/docs/gotutorial)
* [Ginkgo](https://github.com/onsi/ginkgo)
* [Mockery](https://github.com/vektra/mockery)
* Docker (required for funtional tests)

# Example Usage

Create a queue.
```
import (
    "time"
    "github.com/splunk/kafka-mq-go/pkg/kafka"
    "github.com/splunk/kafka-mq-go/queue"
)

service, err := queue.NewService(kafka.NewClientFactory(), Config{
	BootstrapServers: "broker:9092",
	MessageTopic: "messages",
	MarkerTopic: "markers",
    RedeliveryTimeout: 10 * time.Second,
    ConsumerReceiveBufferCapacity: 1,
    ConsumerNextMessageTimeout: 1 * time.Second,
    ConsumerMaxMessagesPerCommit: 1000,
    RedeliveryTracker: RedeliveryTrackerConfig{
        UseNowIfNoMarkerSeen: 10 * time.Second,
        NumOffsetsPerCommit: 1000,
    },
})

q := service.GetOrCreateQueue("test-queue")
```

Create a producer and send a message.
```
p := q.Producer()
err := p.Send([]byte("hi"))
```

Create a consumer and receive a message.
```
c := q.Consumer()
m, err := c.Receive(context.Background())
if string(m.Payload()) == "hi" {
    m.Ack() // ack the message to prevent redelivery
}
```

For more detailed examples see [the set of functional tests](./tests/functional).

# Tests

## Unit Tests
Modules are covered with BDD-style unit tests written using [Ginkgo](https://github.com/onsi/ginkgo), [Gomega](https://github.com/onsi/gomega), and [Mockery](https://github.com/vektra/mockery) for making assertions about expected actions/reactions.  Unit tests can be run via the Makefile:
```
$ make test
```

## Functional Tests

A functional test harness and set of test cases that exercise integration of the system is included under [tests/functional](./tests/functional).  Functional tests require docker to host local instances of ZooKeeper and Kafka and can be run via the Makefile:
```
$ make functional-test
```

## Bench Tests

An initial set of bench test cases that measure performance of the integrated system is included under [tests/bench](./tests/bench).
```
$ make bench-test
```

# License

[Apache 2.0](./License)

# Contributing

Pull requests require the following web form to be submitted in order to be reviewed.

https://www.splunk.com/en_us/form/contributions.html
