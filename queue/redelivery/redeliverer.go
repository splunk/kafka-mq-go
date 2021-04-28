package redelivery

import (
	"context"
	"sync"

	"cd.splunkdev.com/dferstay/kafka-mq-go/pkg/logging"
	"cd.splunkdev.com/dferstay/kafka-mq-go/queue/internal"
	kafkamq "cd.splunkdev.com/dferstay/kafka-mq-go/queue/proto"

	"github.com/cenkalti/backoff"
	confluentKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

const (
	markerLogKey = "marker"
	msgLogKey    = "message"
	maxUint      = ^uint(0)
	maxInt       = int(maxUint >> 1)
)

// Redeliverer provides functionality to redeliver messages associated with markers whose deadlines have expired.
//       with the mechanics of message redelivery.
type Redeliverer interface {
	Redeliver(markers []*kafkamq.Marker)
	Stop()
}

// redeliverer is a thread-safe implementation of the `Redeliverer` insterface
type redeliverer struct {
	wg               sync.WaitGroup
	backOff          backoff.BackOff
	messageSender    internal.MessageSender
	markerProducer   internal.MarkerProducer
	redeliveryCtx    context.Context
	cancelRedelivery context.CancelFunc
	logger           *logging.Logger
}

// NewRedeliverer constructs a `Redeliverer` instance that can be used to redeliver messages associated with markers whose deadlines have expired.
func NewRedeliverer(partition int32, messageSender internal.MessageSender, markerProducer internal.MarkerProducer, backoffPolicy backoff.BackOff) Redeliverer {

	logger := logging.WithComponent("redeliverer").With(logging.Partition, partition)
	logger.Info("creating redeliverer with backoff")

	ctx, cancel := context.WithCancel(context.Background())

	return &redeliverer{
		wg:               sync.WaitGroup{},
		backOff:          backoffPolicy,
		messageSender:    messageSender,
		markerProducer:   markerProducer,
		redeliveryCtx:    ctx,
		cancelRedelivery: cancel,
		logger:           logger,
	}
}

func (r *redeliverer) Redeliver(markers []*kafkamq.Marker) {
	r.logger.Debug("Redeliver", "numMarkers", len(markers))
	for _, m := range markers {
		r.wg.Add(1)
		go r.redeliver(r.redeliveryCtx, m)
	}
}

func (r *redeliverer) Stop() {
	r.logger.Info("Stop")
	r.logger.Debug("cancelling redelivery")
	r.cancelRedelivery()

	r.logger.Info("waiting for background redelivery threads to stop")
	r.wg.Wait()
	r.logger.Info("all redelivery threads have stopped")
}

func (r *redeliverer) redeliver(ctx context.Context, marker *kafkamq.Marker) {
	policy := backoff.WithContext(r.backOff, ctx)

	retryErr := backoff.Retry(func() error {
		err := r.tryRedeliver(marker)
		r.logger.Debug("attempting to redeliver marker", markerLogKey, marker)
		if err != nil {
			r.logger.Error(err, "failed to redeliver marker", logging.ErrorKey, err)
			return err
		}
		return nil
	}, policy)

	if retryErr != nil {
		r.logger.Error(retryErr, "redelivery exceeded exponential backoff limit", markerLogKey, marker)
	}
	r.wg.Done()
}

func (r *redeliverer) tryRedeliver(marker *kafkamq.Marker) error {
	if marker.Type != kafkamq.Marker_START {
		return errors.Errorf("redelivery cannot be performed with a Marker of type %s", marker.Type.String())
	}

	// Determine what queue the message belongs to
	queueID := string(marker.Key)

	// Re-build the original message
	msg := r.messageFromMarker(marker)

	// Perform redelivery
	r.logger.Info("sending message", logging.QueueID, queueID, markerLogKey, marker, msgLogKey, msg)
	if err := r.messageSender.SendMessage(queueID, msg.Value); err != nil {
		return err
	}

	// Send an end marker to denote that processing of the message original message has completed.
	r.sendEndMarker(r.redeliveryCtx, queueID, marker, msg)

	return nil
}

// sendEndMarker retries sending the end marker to the markers topic "forever" (`maxInt` times).
// We use the end marker to denote that processing of the message has completed; the redelivered message
// will be identified by a different partition:offset when its processing begins.
// NOTE: We retry sending the end marker forever because failure to produce an end marker will cause
//       the message to remain in-progress in the redelivery tracker and prevent the marker consumer
//       committed offset from advancing.  Until the marker consumer committed offset advances, we are
//       in a situation where the message could be re-delivered more than once.
//       If a crash/restart or a partition re-assignment of the tracker occurs before the end marker send
//       has completed, the start marker will be re-processed and the message will be redelivered.
func (r *redeliverer) sendEndMarker(ctx context.Context, queueID string, marker *kafkamq.Marker, msg *confluentKafka.Message) {
	r.logger.Info("sending end marker", logging.QueueID, queueID, markerLogKey, marker, msgLogKey, msg)

	policy := backoff.WithContext(r.backOff, ctx)
	retryErr := backoff.Retry(func() error {

		select {
		case <-ctx.Done():
			r.logger.Info("redelivery processing was stopped")
			return nil
		default:
			err := r.markerProducer.SendEndMarker(queueID, msg)
			if err != nil {
				r.logger.Warn("failed to send end marker, retrying", logging.ErrorKey, err, logging.QueueID, queueID, markerLogKey, marker, msgLogKey, msg)
			}
			return err
		}
	}, policy)

	if retryErr != nil {
		r.logger.Error(retryErr, "failed to send end marker, giving up", logging.QueueID, queueID, markerLogKey, marker, msgLogKey, msg)
	}
}

// messageFromMarker rebuilds the original message from the specified marker.
// Note that the provided marker must be a start marker, because only start markers contain the original message contents
func (r *redeliverer) messageFromMarker(marker *kafkamq.Marker) *confluentKafka.Message {
	return &confluentKafka.Message{
		TopicPartition: confluentKafka.TopicPartition{
			Partition: marker.MessageId.Partition,
			Offset:    confluentKafka.Offset(marker.MessageId.Offset),
		},
		Value: marker.Value,
		Key:   marker.Key,
	}
}
