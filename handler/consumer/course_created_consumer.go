package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/ArjunDev17/notification-service/events"
	"github.com/ArjunDev17/notification-service/internal/kafka"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
)

type CourseCreatedConsumer struct {
	consumer *kafka.Consumer
	useCase  *notificationusecase.SendCourseNotificationUseCase
	logger   *slog.Logger
}

func NewCourseCreatedConsumer(
	consumer *kafka.Consumer,
	useCase *notificationusecase.SendCourseNotificationUseCase,
	logger *slog.Logger,
) *CourseCreatedConsumer {

	return &CourseCreatedConsumer{
		consumer: consumer,
		useCase:  useCase,
		logger:   logger,
	}
}

// Start begins consuming messages forever.
func (c *CourseCreatedConsumer) Start(
	ctx context.Context,
) error {

	c.logger.Info(
		"course consumer started",
	)

	for {

		msg, err := c.consumer.ReadMessage(ctx)

		if err != nil {

			return err
		}

		var event events.CourseCreatedEvent

		if err := json.Unmarshal(
			msg.Value,
			&event,
		); err != nil {

			c.logger.Error(
				"failed to deserialize event",
				"error", err,
			)

			continue
		}

		c.logger.Info(
			"course created event received",
			"course_id", event.CourseID,
			"title", event.Title,
		)

		if err := c.useCase.Execute(
			ctx,
			event,
		); err != nil {

			c.logger.Error(
				"failed to process notification",
				"error", err,
			)
		}
	}
}