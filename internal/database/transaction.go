package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	repositorydb "github.com/ArjunDev17/notification-service/repository"
)

func (db *PostgreSQL) BeginTx(
	ctx context.Context,
) (pgx.Tx, error) {

	return db.Pool.BeginTx(
		ctx,
		pgx.TxOptions{},
	)
}

// Executor returns the active database executor.
//
// If a transaction exists in the context,
// it returns pgx.Tx.
//
// Otherwise it returns the connection pool.
func Executor(
	ctx context.Context,
	pool *pgxpool.Pool,
) repositorydb.DBTX {

	if tx, ok := Transaction(ctx); ok {
		return tx
	}

	return pool
}
