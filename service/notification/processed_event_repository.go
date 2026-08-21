package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

// ProcessedEventRepository provides persistence
// operations for idempotency tracking.
type ProcessedEventRepository interface {

	// Create records an event as successfully processed.
	Create(
		ctx context.Context,
		event *domain.ProcessedEvent,
	) error

	// Exists checks whether an event has already
	// been processed.
	Exists(
		ctx context.Context,
		eventID string,
	) (bool, error)
}