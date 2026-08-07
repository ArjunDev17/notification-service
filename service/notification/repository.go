package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

type Repository interface {

	Create(
		ctx context.Context,
		notification *domain.Notification,
	) error

	Update(
		ctx context.Context,
		notification *domain.Notification,
	) error

	FindByID(
		ctx context.Context,
		id string,
	) (*domain.Notification, error)
}