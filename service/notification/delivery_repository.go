package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

// DeliveryRepository defines persistence operations
// for NotificationDelivery.
type DeliveryRepository interface {

	Create(
		ctx context.Context,
		delivery *domain.NotificationDelivery,
	) error

	Update(
		ctx context.Context,
		delivery *domain.NotificationDelivery,
	) error

	FindByNotificationID(
		ctx context.Context,
		notificationID string,
	) ([]*domain.NotificationDelivery, error)

	FindFailed(
		ctx context.Context,
	) ([]*domain.NotificationDelivery, error)
}