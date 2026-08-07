package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

// NotificationRepository defines persistence operations
// for Notification aggregate.
type NotificationRepository interface {
	Create(
		ctx context.Context,
		notification *domain.Notification,
	) error

	FindByID(
		ctx context.Context,
		id string,
	) (*domain.Notification, error)
}
