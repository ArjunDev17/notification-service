package domain

import "time"

// ProcessedEvent represents a Kafka event that has
// already been successfully processed by the service.
//
// It is used to provide application-level idempotency.
type ProcessedEvent struct {
	ID string

	EventID string

	EventType string

	ProcessedAt time.Time
}