package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/internal/mapper"
)

type Service struct {
	notificationRepository NotificationRepository
	deliveryRepository     DeliveryRepository
	dispatcher             Dispatcher
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
	}
}

// Save persists the notification and all delivery records.
// This method MUST be executed inside a UnitOfWork.
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

// Dispatch sends notifications using the configured
// notification channels.
//
// IMPORTANT:
// This method MUST be called AFTER the database
// transaction has been committed.
func (s *Service) Dispatch(
	ctx context.Context,
	bundle *mapper.NotificationBundle,
) error {

	for _, delivery := range bundle.Deliveries {

		if err := s.dispatcher.Dispatch(
			ctx,
			delivery,
			bundle.Notification,
		); err != nil {

			return err
		}
	}

	return nil
}
