package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafkago.Reader
}

// NewConsumer creates a Kafka consumer.
func NewConsumer(
	brokers []string,
	groupID string,
	topic string,
) *Consumer {

	reader := kafkago.NewReader(
		kafkago.ReaderConfig{

			Brokers: brokers,

			GroupID: groupID,

			Topic: topic,

			MinBytes: 10e3,

			MaxBytes: 10e6,
		},
	)

	return &Consumer{
		reader: reader,
	}
}

// ReadMessage reads one message from Kafka.
func (c *Consumer) ReadMessage(
	ctx context.Context,
) (kafkago.Message, error) {

	return c.reader.ReadMessage(ctx)
}

// Close gracefully closes Kafka consumer.
func (c *Consumer) Close() error {

	return c.reader.Close()
}
