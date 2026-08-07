package consumer

import (
	"context"

	"github.com/ArjunDev17/notification-service/events"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
	"github.com/ArjunDev17/notification-service/internal/worker"
)

// NotificationJob represents one Kafka event that
// will be processed asynchronously by the worker pool.
type NotificationJob struct {

	// Kafka Event
	event events.CourseCreatedEvent

	// Business Use Case
	useCase *notificationusecase.ProcessCourseCreatedUseCase

	// Completion notification
	result chan worker.Result
}

// NewNotificationJob creates a new worker job.
func NewNotificationJob(
	event events.CourseCreatedEvent,
	useCase *notificationusecase.ProcessCourseCreatedUseCase,
) *NotificationJob {

	return &NotificationJob{

		event: event,

		useCase: useCase,

		// Buffered channel avoids worker blocking
		result: make(
			chan worker.Result,
			1,
		),
	}
}

// Execute is called by the worker pool.
func (j *NotificationJob) Execute(
	ctx context.Context,
) error {

	err := j.useCase.Execute(
		ctx,
		j.event,
	)

	// Notify whoever submitted this job.
	j.result <- worker.Result{
		Err: err,
	}

	return err
}

// Result returns a read-only result channel.
func (j *NotificationJob) Result() <-chan worker.Result {

	return j.result
}