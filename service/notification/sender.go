package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

type Sender interface {

	Channel() domain.DeliveryChannel

	Send(
		ctx context.Context,
		delivery *domain.NotificationDelivery,
		notification *domain.Notification,
	) error
}