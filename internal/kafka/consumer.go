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

			// Minimum amount of data to fetch.
			MinBytes: 10e3,

			// Maximum amount of data to fetch.
			MaxBytes: 10e6,
		},
	)

	return &Consumer{
		reader: reader,
	}
}

// FetchMessage reads a message WITHOUT committing its offset.
func (c *Consumer) FetchMessage(
	ctx context.Context,
) (kafkago.Message, error) {

	return c.reader.FetchMessage(ctx)
}

// CommitMessage commits the processed offset.
func (c *Consumer) CommitMessage(
	ctx context.Context,
	message kafkago.Message,
) error {

	return c.reader.CommitMessages(
		ctx,
		message,
	)
}

// Close gracefully closes the Kafka consumer.
func (c *Consumer) Close() error {

	return c.reader.Close()
}