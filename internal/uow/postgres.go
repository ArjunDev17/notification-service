package uow

import (
	"context"

	"github.com/ArjunDev17/notification-service/internal/database"
)

type postgresUnitOfWork struct {
	db *database.PostgreSQL
}

func NewPostgres(
	db *database.PostgreSQL,
) UnitOfWork {

	return &postgresUnitOfWork{
		db: db,
	}
}

func (u *postgresUnitOfWork) Do(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {

	tx, err := u.db.BeginTx(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	txCtx := database.WithTransaction(
		ctx,
		tx,
	)

	if err := fn(txCtx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
