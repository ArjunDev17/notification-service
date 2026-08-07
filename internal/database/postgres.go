package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ArjunDev17/notification-service/internal/config"
)

type PostgreSQL struct {
	Pool *pgxpool.Pool
}

func New(
	cfg config.DatabaseConfig,
) (*PostgreSQL, error) {

	pool, err := pgxpool.New(
		context.Background(),
		cfg.DSN(),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"create postgres pool: %w",
			err,
		)
	}

	if err := pool.Ping(context.Background()); err != nil {

		return nil, fmt.Errorf(
			"ping postgres: %w",
			err,
		)
	}

	return &PostgreSQL{
		Pool: pool,
	}, nil
}

func (db *PostgreSQL) Close() {
	db.Pool.Close()
}