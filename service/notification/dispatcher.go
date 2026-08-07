package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

type Dispatcher interface {
	Dispatch(
		ctx context.Context,
		delivery *domain.NotificationDelivery,
		notification *domain.Notification,
	) error
}
