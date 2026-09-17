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

// Create stores an event as processed.
//
// event_id has a UNIQUE constraint in PostgreSQL,
// therefore duplicate events cannot be inserted.
func (r *processedEventRepository) Create(
	ctx context.Context,
	event *domain.ProcessedEvent,
) error {

	query := `
	INSERT INTO processed_events
	(
		id,
		event_id,
		event_type,
		processed_at
	)
	VALUES
	(
		$1,
		$2,
		$3,
		$4
	)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		event.ID,
		event.EventID,
		event.EventType,
		event.ProcessedAt,
	)

	return err
}

// Exists checks whether the Kafka event has already
// been processed.
func (r *processedEventRepository) Exists(
	ctx context.Context,
	eventID string,
) (bool, error) {

	query := `
	SELECT EXISTS (
		SELECT 1
		FROM processed_events
		WHERE event_id = $1
	)
	`

	var exists bool

	err := r.db.QueryRow(
		ctx,
		query,
		eventID,
	).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf(
			"check processed event %s: %w",
			eventID,
			err,
		)
	}

	return exists, nil
}
