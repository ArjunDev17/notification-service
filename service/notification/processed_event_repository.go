
package notification

import (
	"context"

	"github.com/ArjunDev17/notification-service/domain"
)

// ProcessedEventRepository atomically claims events
// for idempotent processing.
type ProcessedEventRepository interface {
	TryCreate(
		ctx context.Context,
		event *domain.ProcessedEvent,
	) (bool, error)
}
