package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type DLQPublisher struct {
	writer *kafkago.Writer
}

func NewDLQPublisher(
	brokers []string,
	topic string,
) *DLQPublisher {
	return &DLQPublisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireAll,
			Async:        false,
		},
	}
}

func (p *DLQPublisher) Publish(
	ctx context.Context,
	original kafkago.Message,
	cause error,
) error {
	if cause == nil {
		return fmt.Errorf("DLQ failure cause cannot be nil")
	}

	failedAt := time.Now().UTC().Format(time.RFC3339Nano)

	dlqMessage := kafkago.Message{
		Key:   original.Key,
		Value: original.Value,
		Time:  time.Now().UTC(),
		Headers: []kafkago.Header{
			{Key: "source-topic", Value: []byte(original.Topic)},
			{Key: "source-partition", Value: []byte(strconv.Itoa(original.Partition))},
			{Key: "source-offset", Value: []byte(strconv.FormatInt(original.Offset, 10))},
			{Key: "failure-reason", Value: []byte(cause.Error())},
			{Key: "failure-time", Value: []byte(failedAt)},
		},
	}

	if err := p.writer.WriteMessages(ctx, dlqMessage); err != nil {
		return fmt.Errorf("write message to DLQ: %w", err)
	}

	return nil
}

func (p *DLQPublisher) Close() error {
	return p.writer.Close()
}
