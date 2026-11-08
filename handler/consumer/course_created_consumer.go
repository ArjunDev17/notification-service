package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ArjunDev17/notification-service/events"
	kafkaclient "github.com/ArjunDev17/notification-service/internal/kafka"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
	"github.com/ArjunDev17/notification-service/internal/worker"

	kafkago "github.com/segmentio/kafka-go"
)

type CourseCreatedConsumer struct {
	consumer     *kafkaclient.Consumer
	dlqPublisher *kafkaclient.DLQPublisher

	workerPool *worker.Pool
	useCase    *notificationusecase.ProcessCourseCreatedUseCase

	logger *slog.Logger

	maxRetry          int
	retryDelaySeconds int
}

func NewCourseCreatedConsumer(
	consumer *kafkaclient.Consumer,
	dlqPublisher *kafkaclient.DLQPublisher,
	workerPool *worker.Pool,
	useCase *notificationusecase.ProcessCourseCreatedUseCase,
	logger *slog.Logger,
	maxRetry int,
	retryDelaySeconds int,
) *CourseCreatedConsumer {
	return &CourseCreatedConsumer{
		consumer:          consumer,
		dlqPublisher:      dlqPublisher,
		workerPool:        workerPool,
		useCase:           useCase,
		logger:            logger,
		maxRetry:          maxRetry,
		retryDelaySeconds: retryDelaySeconds,
	}
}

// Start begins consuming Kafka messages.
func (c *CourseCreatedConsumer) Start(ctx context.Context) error {
	c.logger.Info("starting course.created consumer")

	c.workerPool.Start(ctx)

	defer c.workerPool.Shutdown()

	for {
		if ctx.Err() != nil {
			c.logger.Info("consumer shutting down")
			return nil
		}

		// STEP 1: Fetch Kafka message.
		message, err := c.consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			c.logger.Error(
				"failed to fetch kafka message",
				"error", err,
			)

			if ctx.Err() != nil {
				return nil
			}

			continue
		}

		// STEP 2: Deserialize event.
		var event events.CourseCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			c.logger.Error(
				"invalid course.created event",
				"error", err,
				"offset", message.Offset,
				"partition", message.Partition,
			)

			if dlqErr := c.sendToDLQ(ctx, message, err); dlqErr != nil {
				c.logger.Error(
					"failed to send invalid message to DLQ",
					"error", dlqErr,
					"offset", message.Offset,
				)

				if ctx.Err() != nil {
					return nil
				}

				continue
			}

			c.logger.Info(
				"invalid message successfully sent to DLQ",
				"offset", message.Offset,
				"partition", message.Partition,
			)

			continue
		}

		// STEP 3: Process event with bounded retries.
		err = c.processWithRetry(
			ctx,
			event,
		)

		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			c.logger.Error(
				"notification processing exhausted retries",
				"course_id", event.CourseID,
				"error", err,
			)

			// STEP 4: Publish exhausted message to DLQ.
			if dlqErr := c.sendToDLQ(ctx, message, err); dlqErr != nil {
				c.logger.Error(
					"failed to send exhausted message to DLQ",
					"course_id", event.CourseID,
					"error", dlqErr,
				)

				if ctx.Err() != nil {
					return nil
				}

				// Do not commit. Kafka can redeliver.
				continue
			}

			c.logger.Warn(
				"message moved to DLQ after retries",
				"course_id", event.CourseID,
				"offset", message.Offset,
			)

			continue
		}

		// STEP 5: Commit successful message.
		if err := c.consumer.CommitMessage(ctx, message); err != nil {
			c.logger.Error(
				"failed to commit kafka offset",
				"error", err,
				"offset", message.Offset,
			)

			continue
		}

		c.logger.Info(
			"course.created processed successfully",
			"course_id", event.CourseID,
			"offset", message.Offset,
			"partition", message.Partition,
		)
	}
}

// processWithRetry attempts to process the event a bounded number of times.
// MaxRetry means retries after the initial attempt.
func (c *CourseCreatedConsumer) processWithRetry(
	ctx context.Context,
	event events.CourseCreatedEvent,
) error {
	totalAttempts := c.maxRetry + 1

	if totalAttempts < 1 {
		totalAttempts = 1
	}

	retryDelay := time.Duration(c.retryDelaySeconds) * time.Second

	for attempt := 1; attempt <= totalAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.logger.Info(
			"processing notification event",
			"course_id", event.CourseID,
			"attempt", attempt,
			"max_attempts", totalAttempts,
		)

		job := NewNotificationJob(
			event,
			c.useCase,
		)

		c.workerPool.Submit(job)

		select {
		case result := <-job.Result():
			if result.Err == nil {
				return nil
			}

			c.logger.Error(
				"notification processing failed",
				"course_id", event.CourseID,
				"attempt", attempt,
				"error", result.Err,
			)

			if attempt == totalAttempts {
				return fmt.Errorf(
					"processing failed after %d attempts: %w",
					totalAttempts,
					result.Err,
				)
			}

		case <-ctx.Done():
			return ctx.Err()
		}

		if retryDelay <= 0 {
			continue
		}

		timer := time.NewTimer(retryDelay)

		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			return ctx.Err()
		}
	}

	return fmt.Errorf("notification processing failed")
}

// sendToDLQ publishes the original message before committing its offset.
func (c *CourseCreatedConsumer) sendToDLQ(
	ctx context.Context,
	message kafkago.Message,
	cause error,
) error {
	if err := c.dlqPublisher.Publish(ctx, message, cause); err != nil {
		return fmt.Errorf("publish to DLQ: %w", err)
	}

	if err := c.consumer.CommitMessage(ctx, message); err != nil {
		return fmt.Errorf(
			"commit original offset after DLQ publishing: %w",
			err,
		)
	}

	return nil
}
