package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// DLQPublisher publishes failed messages to a Dead Letter Queue.
type DLQPublisher struct {
	writer *kafkago.Writer
}

// NewDLQPublisher creates a synchronous Kafka publisher.
// A successful return from Publish confirms the broker acknowledged
// the write according to the configured acknowledgment policy.
func NewDLQPublisher(
	brokers []string,
	topic string,
) *DLQPublisher {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
	}

	return &DLQPublisher{
		writer: writer,
	}
}

// Publish preserves the original key and payload and adds
// source and failure information in Kafka headers.
func (p *DLQPublisher) Publish(
	ctx context.Context,
	original kafkago.Message,
	cause error,
) error {
	if cause == nil {
		return fmt.Errorf("cannot publish DLQ message without a failure cause")
	}

	failedAt := time.Now().UTC().Format(time.RFC3339Nano)

	message := kafkago.Message{
		Key:   original.Key,
		Value: original.Value,
		Time:  time.Now().UTC(),
		Headers: []kafkago.Header{
			{
				Key:   "dlq-source-topic",
				Value: []byte(original.Topic),
			},
			{
				Key:   "dlq-source-partition",
				Value: []byte(strconv.Itoa(original.Partition)),
			},
			{
				Key:   "dlq-source-offset",
				Value: []byte(strconv.FormatInt(original.Offset, 10)),
			},
			{
				Key:   "dlq-error",
				Value: []byte(cause.Error()),
			},
			{
				Key:   "dlq-failed-at",
				Value: []byte(failedAt),
			},
		},
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish message to DLQ: %w", err)
	}

	return nil
}

// Close releases the Kafka writer resources.
func (p *DLQPublisher) Close() error {
	return p.writer.Close()
}
