package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ArjunDev17/notification-service/events"
	kafkaclient "github.com/ArjunDev17/notification-service/internal/kafka"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
	"github.com/ArjunDev17/notification-service/internal/worker"

	kafkago "github.com/segmentio/kafka-go"
)

type CourseCreatedConsumer struct {
	consumer *kafkaclient.Consumer

	workerPool *worker.Pool

	useCase *notificationusecase.ProcessCourseCreatedUseCase

	logger *slog.Logger

	dlqPublisher *kafkaclient.DLQPublisher
}

func NewCourseCreatedConsumer(
	consumer *kafkaclient.Consumer,
	dlqPublisher *kafkaclient.DLQPublisher,
	workerPool *worker.Pool,
	useCase *notificationusecase.ProcessCourseCreatedUseCase,
	logger *slog.Logger,
) *CourseCreatedConsumer {
	return &CourseCreatedConsumer{
		consumer:     consumer,
		dlqPublisher: dlqPublisher,
		workerPool:   workerPool,
		useCase:      useCase,
		logger:       logger,
	}
}

func (c *CourseCreatedConsumer) Start(
	ctx context.Context,
) error {

	c.logger.Info("starting course.created consumer")

	// Start worker pool only once.
	c.workerPool.Start(ctx)

	for {

		select {

		case <-ctx.Done():

			c.logger.Info("consumer shutting down")

			c.workerPool.Shutdown()

			return nil

		default:
		}

		// -------------------------------------------------
		// STEP 1 : Fetch Kafka message
		// -------------------------------------------------

		message, err := c.consumer.FetchMessage(ctx)

		if err != nil {

			if errors.Is(err, context.Canceled) {
				return nil
			}

			c.logger.Error(
				"failed to fetch kafka message",
				"error",
				err,
			)

			continue
		}

		// -------------------------------------------------
		// STEP 2 : Deserialize JSON
		// -------------------------------------------------

		var event events.CourseCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			c.logger.Error(
				"invalid course.created event; sending to DLQ",
				"error", err,
				"offset", message.Offset,
				"partition", message.Partition,
			)

			if dlqErr := c.sendToDLQ(ctx, message, err); dlqErr != nil {
				c.logger.Error(
					"failed to handle poison message",
					"error", dlqErr,
					"offset", message.Offset,
					"partition", message.Partition,
				)

				// Do not commit if publishing to the DLQ failed.
				// Kafka can redeliver the original message.
				if ctx.Err() != nil {
					return nil
				}

				continue
			}

			c.logger.Info(
				"invalid message sent to DLQ",
				"offset", message.Offset,
				"partition", message.Partition,
			)

			continue
		}

		// -------------------------------------------------
		// STEP 3 : Create Worker Job
		// -------------------------------------------------

		job := NewNotificationJob(
			event,
			c.useCase,
		)

		// -------------------------------------------------
		// STEP 4 : Submit to Worker Pool
		// -------------------------------------------------

		c.workerPool.Submit(job)

		// -------------------------------------------------
		// STEP 5 : Wait for Completion
		// -------------------------------------------------

		result := <-job.Result()

		if result.Err != nil {

			c.logger.Error(
				"notification processing failed",
				"course_id",
				event.CourseID,
				"error",
				result.Err,
			)

			// Don't commit.
			// Kafka will redeliver.
			continue
		}

		// -------------------------------------------------
		// STEP 6 : Commit Offset
		// -------------------------------------------------

		if err := c.consumer.CommitMessage(
			ctx,
			message,
		); err != nil {

			c.logger.Error(
				"failed to commit kafka offset",
				"error",
				err,
			)

			continue
		}

		c.logger.Info(
			"course.created processed successfully",
			"course_id",
			event.CourseID,
			"offset",
			message.Offset,
			"partition",
			message.Partition,
		)
	}
}
func (c *CourseCreatedConsumer) sendToDLQ(
	ctx context.Context,
	message kafkago.Message,
	cause error,
) error {
	if err := c.dlqPublisher.Publish(ctx, message, cause); err != nil {
		return err
	}

	if err := c.consumer.CommitMessage(ctx, message); err != nil {
		return fmt.Errorf("commit original message after DLQ publish: %w", err)
	}

	return nil
}
