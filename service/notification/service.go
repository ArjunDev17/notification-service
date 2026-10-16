package notification

import (
	"context"
	"time"

	"github.com/ArjunDev17/notification-service/domain"
	"github.com/ArjunDev17/notification-service/internal/mapper"
)

type Service struct {
	notificationRepository NotificationRepository
	deliveryRepository     DeliveryRepository
	dispatcher             Dispatcher

	// Retry configuration.
	maxRetry   int
	retryDelay time.Duration
}

func NewService(
	notificationRepository NotificationRepository,
	deliveryRepository DeliveryRepository,
	dispatcher Dispatcher,
) *Service {

	return &Service{
		notificationRepository: notificationRepository,
		deliveryRepository:     deliveryRepository,
		dispatcher:             dispatcher,

		// Default retry configuration.
		// Later we'll move these into config.go.
		maxRetry:   3,
		retryDelay: 2 * time.Second,
	}
}

// Save persists the notification and delivery records.
// MUST be executed inside UnitOfWork.
func (s *Service) Save(
	ctx context.Context,
	bundle *mapper.NotificationBundle,
) error {

	//-------------------------------------------------------
	// Save Notification
	//-------------------------------------------------------

	if err := s.notificationRepository.Create(
		ctx,
		bundle.Notification,
	); err != nil {

		return err
	}

	//-------------------------------------------------------
	// Save Deliveries
	//-------------------------------------------------------

	for _, delivery := range bundle.Deliveries {

		if err := s.deliveryRepository.Create(
			ctx,
			delivery,
		); err != nil {

			return err
		}
	}

	return nil
}

// Dispatch sends notifications AFTER
// database transaction is committed.
func (s *Service) Dispatch(
	ctx context.Context,
	bundle *mapper.NotificationBundle,
) error {

	for _, delivery := range bundle.Deliveries {

		if err := s.processDelivery(
			ctx,
			delivery,
			bundle.Notification,
		); err != nil {

			return err
		}
	}

	return nil
}

// retryDelivery retries sending notification.
//
// Retry Flow:
//
// PENDING
//      ↓
// Dispatch()
//      ↓
// FAILED
//      ↓
// RETRYING
//      ↓
// Dispatch()
//      ↓
// DELIVERED / FAILED
func (s *Service) retryDelivery(
	ctx context.Context,
	delivery *domain.NotificationDelivery,
	notification *domain.Notification,
) error {

	for attempt := 1; attempt <= s.maxRetry; attempt++ {

		//---------------------------------------------------
		// Update retry state
		//---------------------------------------------------

		delivery.IncrementRetry()

		if err := s.deliveryRepository.Update(
			ctx,
			delivery,
		); err != nil {

			return err
		}

		//---------------------------------------------------
		// Wait before retry
		//---------------------------------------------------

		select {

		case <-ctx.Done():

			return ctx.Err()

		case <-time.After(s.retryDelay):
		}

		//---------------------------------------------------
		// Retry dispatch
		//---------------------------------------------------

		if err := s.dispatcher.Dispatch(
			ctx,
			delivery,
			notification,
		); err == nil {

			delivery.MarkDelivered()

			return s.deliveryRepository.Update(
				ctx,
				delivery,
			)
		}
	}

	//-------------------------------------------------------
	// All retries exhausted
	//-------------------------------------------------------

	delivery.MarkFailed(
		"maximum retry attempts exceeded",
	)

	return s.deliveryRepository.Update(
		ctx,
		delivery,
	)
}

func (s *Service) processDelivery(
	ctx context.Context,
	delivery *domain.NotificationDelivery,
	notification *domain.Notification,
) error {

	//-------------------------------------------------------
	// First Attempt
	//-------------------------------------------------------

	if err := s.dispatcher.Dispatch(
		ctx,
		delivery,
		notification,
	); err != nil {

		return s.retryDelivery(
			ctx,
			delivery,
			notification,
		)
	}

	//-------------------------------------------------------
	// Success
	//-------------------------------------------------------

	delivery.MarkDelivered()

	return s.deliveryRepository.Update(
		ctx,
		delivery,
	)
}