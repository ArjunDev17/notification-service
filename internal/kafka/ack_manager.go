package kafka

import (
	"context"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
)

type Ack struct {
	Message kafkago.Message
	Err     error
}

type AckManager struct {
	consumer *Consumer

	logger *slog.Logger

	queue chan Ack
}

func NewAckManager(
	consumer *Consumer,
	logger *slog.Logger,
	queueSize int,
) *AckManager {

	return &AckManager{

		consumer: consumer,

		logger: logger,

		queue: make(
			chan Ack,
			queueSize,
		),
	}
}

// AckQueue returns a write-only acknowledgement channel.
func (m *AckManager) AckQueue() chan<- Ack {

	return m.queue
}

func (m *AckManager) Start(
	ctx context.Context,
) {

	go func() {

		for {

			select {

			case <-ctx.Done():

				return

			case ack := <-m.queue:

				if ack.Err != nil {

					m.logger.Error(
						"message processing failed, offset not committed",
						"error",
						ack.Err,
					)

					continue
				}

				if err := m.consumer.CommitMessage(
					ctx,
					ack.Message,
				); err != nil {

					m.logger.Error(
						"offset commit failed",
						"error",
						err,
					)

					continue
				}

				m.logger.Info(
					"offset committed",
					"offset",
					ack.Message.Offset,
				)
			}
		}
	}()
}
