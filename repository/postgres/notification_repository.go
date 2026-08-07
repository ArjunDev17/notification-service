package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ArjunDev17/notification-service/domain"
	"github.com/ArjunDev17/notification-service/internal/database"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

type notificationRepository struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(
	pool *pgxpool.Pool,
) notificationservice.NotificationRepository {

	return &notificationRepository{
		pool: pool,
	}
}

func (r *notificationRepository) Create(
	ctx context.Context,
	notification *domain.Notification,
) error {

	query := `
	INSERT INTO notifications
	(
		id,
		type,
		title,
		message,
		metadata,
		created_at
	)
	VALUES
	(
		$1,$2,$3,$4,$5,$6
	)
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	_, err := exec.Exec(
		ctx,
		query,
		notification.ID,
		notification.Type,
		notification.Title,
		notification.Message,
		notification.Metadata,
		notification.CreatedAt,
	)

	return err
}

func (r *notificationRepository) FindByID(
	ctx context.Context,
	id string,
) (*domain.Notification, error) {

	query := `
	SELECT
		id,
		type,
		title,
		message,
		metadata,
		created_at
	FROM notifications
	WHERE id = $1
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	var notification domain.Notification

	err := exec.QueryRow(
		ctx,
		query,
		id,
	).Scan(
		&notification.ID,
		&notification.Type,
		&notification.Title,
		&notification.Message,
		&notification.Metadata,
		&notification.CreatedAt,
	)

	if err != nil {
		return nil, err
	}

	return &notification, nil
}
