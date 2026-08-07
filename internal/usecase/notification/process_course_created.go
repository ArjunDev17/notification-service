package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/events"
	"github.com/ArjunDev17/notification-service/internal/mapper"
	"github.com/ArjunDev17/notification-service/internal/uow"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

type ProcessCourseCreatedUseCase struct {
	uow                 uow.UnitOfWork
	notificationService *notificationservice.Service
}

func NewProcessCourseCreatedUseCase(
	uow uow.UnitOfWork,
	notificationService *notificationservice.Service,
) *ProcessCourseCreatedUseCase {

	return &ProcessCourseCreatedUseCase{
		uow:                 uow,
		notificationService: notificationService,
	}
}

func (uc *ProcessCourseCreatedUseCase) Execute(
	ctx context.Context,
	event events.CourseCreatedEvent,
) error {

	//------------------------------------------------------
	// Step 1 : Convert Kafka Event -> Domain Model
	//------------------------------------------------------

	bundle := mapper.ToNotificationBundle(
		event,
	)

	//------------------------------------------------------
	// Step 2 : Persist inside one transaction
	//------------------------------------------------------

	if err := uc.uow.Do(
		ctx,
		func(txCtx context.Context) error {

			return uc.notificationService.Save(
				txCtx,
				bundle,
			)
		},
	); err != nil {

		return err
	}

	//------------------------------------------------------
	// Step 3 : Dispatch after successful commit
	//------------------------------------------------------

	if err := uc.notificationService.Dispatch(
		ctx,
		bundle,
	); err != nil {

		return err
	}

	return nil
}
