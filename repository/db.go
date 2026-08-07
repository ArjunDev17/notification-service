package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgconn.CommandTag, error)

	Query(
		ctx context.Context,
		sql string,
		arguments ...any,
	) (pgx.Rows, error)

	QueryRow(
		ctx context.Context,
		sql string,
		arguments ...any,
	) pgx.Row
}
