package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ArjunDev17/notification-service/domain"
	"github.com/ArjunDev17/notification-service/internal/database"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

type deliveryRepository struct {
	pool *pgxpool.Pool
}

func NewDeliveryRepository(
	pool *pgxpool.Pool,
) notificationservice.DeliveryRepository {
	return &deliveryRepository{
		pool: pool,
	}
}

func (r *deliveryRepository) Create(ctx context.Context, delivery *domain.NotificationDelivery) error {

	query := `
	INSERT INTO notification_deliveries
	(
		id,
		notification_id,
		channel,
		recipient,
		status,
		retry_count,
		error_message,
		sent_at,
		delivered_at,
		failed_at,
		created_at,
		updated_at
	)
	VALUES
	(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW()
	)
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	_, err := exec.Exec(
		ctx,
		query,
		delivery.ID,
		delivery.NotificationID,
		delivery.Channel,
		delivery.Recipient,
		delivery.Status,
		delivery.RetryCount,
		delivery.ErrorMessage,
		delivery.SentAt,
		delivery.DeliveredAt,
		delivery.FailedAt,
	)

	return err
}

func (r *deliveryRepository) Update(
	ctx context.Context,
	delivery *domain.NotificationDelivery,
) error {

	query := `
	UPDATE notification_deliveries
	SET
		status=$1,
		retry_count=$2,
		error_message=$3,
		sent_at=$4,
		delivered_at=$5,
		failed_at=$6,
		updated_at=NOW()
	WHERE id=$7
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	_, err := exec.Exec(
		ctx,
		query,
		delivery.Status,
		delivery.RetryCount,
		delivery.ErrorMessage,
		delivery.SentAt,
		delivery.DeliveredAt,
		delivery.FailedAt,
		delivery.ID,
	)

	return err
}

func (r *deliveryRepository) FindByNotificationID(
	ctx context.Context,
	notificationID string,
) ([]*domain.NotificationDelivery, error) {

	query := `
	SELECT
		id,
		notification_id,
		channel,
		recipient,
		status,
		retry_count,
		error_message,
		sent_at,
		delivered_at,
		failed_at
	FROM notification_deliveries
	WHERE notification_id=$1
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	rows, err := exec.Query(
		ctx,
		query,
		notificationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deliveries := make([]*domain.NotificationDelivery, 0)

	for rows.Next() {

		var delivery domain.NotificationDelivery

		if err := rows.Scan(
			&delivery.ID,
			&delivery.NotificationID,
			&delivery.Channel,
			&delivery.Recipient,
			&delivery.Status,
			&delivery.RetryCount,
			&delivery.ErrorMessage,
			&delivery.SentAt,
			&delivery.DeliveredAt,
			&delivery.FailedAt,
		); err != nil {
			return nil, err
		}

		deliveries = append(
			deliveries,
			&delivery,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return deliveries, nil
}

func (r *deliveryRepository) FindFailed(
	ctx context.Context,
) ([]*domain.NotificationDelivery, error) {

	query := `
	SELECT
		id,
		notification_id,
		channel,
		recipient,
		status,
		retry_count,
		error_message,
		sent_at,
		delivered_at,
		failed_at
	FROM notification_deliveries
	WHERE status='FAILED'
	`

	exec := database.Executor(
		ctx,
		r.pool,
	)

	rows, err := exec.Query(
		ctx,
		query,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deliveries := make([]*domain.NotificationDelivery, 0)

	for rows.Next() {

		var delivery domain.NotificationDelivery

		if err := rows.Scan(
			&delivery.ID,
			&delivery.NotificationID,
			&delivery.Channel,
			&delivery.Recipient,
			&delivery.Status,
			&delivery.RetryCount,
			&delivery.ErrorMessage,
			&delivery.SentAt,
			&delivery.DeliveredAt,
			&delivery.FailedAt,
		); err != nil {
			return nil, err
		}

		deliveries = append(
			deliveries,
			&delivery,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return deliveries, nil
}
