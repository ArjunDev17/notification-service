package database

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// transactionKey is an unexported type used as the
// context key for storing the current transaction.
type transactionKey struct{}

// WithTransaction returns a new context that contains
// the active PostgreSQL transaction.
func WithTransaction(
	ctx context.Context,
	tx pgx.Tx,
) context.Context {

	return context.WithValue(
		ctx,
		transactionKey{},
		tx,
	)
}

// Transaction retrieves the transaction from the context.
//
// Returns:
//   - pgx.Tx : active transaction
//   - bool   : true if a transaction exists
func Transaction(
	ctx context.Context,
) (pgx.Tx, bool) {

	tx, ok := ctx.Value(
		transactionKey{},
	).(pgx.Tx)

	return tx, ok
}