package domain

import "time"

// NotificationStatus represents the current state
// of a notification.
type NotificationStatus string

const (
	StatusPending NotificationStatus = "PENDING"

	StatusSending NotificationStatus = "SENDING"

	StatusSent NotificationStatus = "SENT"

	StatusFailed NotificationStatus = "FAILED"

	StatusRead NotificationStatus = "READ"
)

// Notification is the core business entity
// of the Notification Service.
type Notification struct {
	ID string

	Type string

	Title string

	Message string

	// Additional event-specific information.
	// Stored as JSONB in PostgreSQL.
	Metadata map[string]any

	CreatedAt time.Time
}
