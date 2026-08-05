package notification

import (
	"context"
	"log/slog"

	"github.com/ArjunDev17/notification-service/events"
)

type SendCourseNotificationUseCase struct {
	logger *slog.Logger
}

func NewSendCourseNotificationUseCase(
	logger *slog.Logger,
) *SendCourseNotificationUseCase {

	return &SendCourseNotificationUseCase{
		logger: logger,
	}
}

func (u *SendCourseNotificationUseCase) Execute(
	ctx context.Context,
	event events.CourseCreatedEvent,
) error {

	u.logger.Info(
		"processing notification",
		"course_id", event.CourseID,
		"title", event.Title,
	)

	return nil
}
