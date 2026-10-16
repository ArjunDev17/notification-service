
package postgres

import (
	"context"
	"fmt"

	"github.com/ArjunDev17/notification-service/domain"
	repositorydb "github.com/ArjunDev17/notification-service/repository"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

type processedEventRepository struct {
	db repositorydb.DBTX
}

func NewProcessedEventRepository(
	db repositorydb.DBTX,
) notificationservice.ProcessedEventRepository {
	return &processedEventRepository{
		db: db,
	}
}

func (r *processedEventRepository) TryCreate(
	ctx context.Context,
	event *domain.ProcessedEvent,
) (bool, error) {
	query := `
		INSERT INTO processed_events (
			id,
			event_id,
			event_type,
			processed_at
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO NOTHING
	`

	tag, err := r.db.Exec(
		ctx,
		query,
		event.ID,
		event.EventID,
		event.EventType,
		event.ProcessedAt,
	)
	if err != nil {
		return false, fmt.Errorf(
			"claim processed event %s: %w",
			event.EventID,
			err,
		)
	}

	return tag.RowsAffected() == 1, nil
}
