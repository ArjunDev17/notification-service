package notification

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/ArjunDev17/notification-service/domain"
	"github.com/ArjunDev17/notification-service/events"
	"github.com/ArjunDev17/notification-service/internal/mapper"
	"github.com/ArjunDev17/notification-service/internal/uow"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

type ProcessCourseCreatedUseCase struct {
	uow                      uow.UnitOfWork
	notificationService      *notificationservice.Service
	processedEventRepository notificationservice.ProcessedEventRepository
}

func NewProcessCourseCreatedUseCase(
	uow uow.UnitOfWork,
	notificationService *notificationservice.Service,
	processedEventRepository notificationservice.ProcessedEventRepository,
) *ProcessCourseCreatedUseCase {

	return &ProcessCourseCreatedUseCase{
		uow:                      uow,
		notificationService:      notificationService,
		processedEventRepository: processedEventRepository,
	}
}

func (uc *ProcessCourseCreatedUseCase) Execute(
	ctx context.Context,
	event events.CourseCreatedEvent,
) error {
	bundle := mapper.ToNotificationBundle(event)

	processedEvent := &domain.ProcessedEvent{
		ID:          uuid.NewString(),
		EventID:     event.EventID,
		EventType:   event.EventType,
		ProcessedAt: time.Now(),
	}

	alreadyProcessed := false

	// Claim the event and persist business data atomically.
	err := uc.uow.Do(ctx, func(txCtx context.Context) error {
		claimed, err := uc.processedEventRepository.TryCreate(
			txCtx,
			processedEvent,
		)
		if err != nil {
			return err
		}

		if !claimed {
			alreadyProcessed = true
			return nil
		}

		return uc.notificationService.Save(txCtx, bundle)
	})
	if err != nil {
		return err
	}

	// A duplicate event must not create or dispatch
	// another notification.
	if alreadyProcessed {
		return nil
	}

	// External dispatch happens after the DB transaction commits.
	return uc.notificationService.Dispatch(ctx, bundle)
}
