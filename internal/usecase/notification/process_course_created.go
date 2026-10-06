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

	//------------------------------------------------------
	// Step 1 : Check Idempotency
	//------------------------------------------------------

	processed, err := uc.processedEventRepository.Exists(
		ctx,
		event.EventID,
	)

	if err != nil {
		return err
	}

	if processed {

		// Event was already processed.
		// We return nil so the Kafka consumer
		// can safely commit the offset.
		return nil
	}

	//------------------------------------------------------
	// Step 2 : Convert Kafka Event -> Domain Model
	//------------------------------------------------------

	bundle := mapper.ToNotificationBundle(
		event,
	)

	//------------------------------------------------------
	// Step 3 : Persist Notification + Delivery
	//          + Processed Event
	//------------------------------------------------------

	if err := uc.uow.Do(
		ctx,
		func(txCtx context.Context) error {

			// Save notification and deliveries.
			if err := uc.notificationService.Save(
				txCtx,
				bundle,
			); err != nil {
				return err
			}

			// Mark Kafka event as processed.
			processedEvent := &domain.ProcessedEvent{
				ID:          uuid.NewString(),
				EventID:     event.EventID,
				EventType:   event.EventType,
				ProcessedAt: time.Now(),
			}

			if err := uc.processedEventRepository.Create(
				txCtx,
				processedEvent,
			); err != nil {
				return err
			}

			return nil
		},
	); err != nil {
		return err
	}

	//------------------------------------------------------
	// Step 4 : Dispatch after successful DB commit
	//------------------------------------------------------

	if err := uc.notificationService.Dispatch(
		ctx,
		bundle,
	); err != nil {
		return err
	}

	return nil
}
