package events

import "time"

// CourseCreatedEvent represents the event published
// by the Course Content Service.
type CourseCreatedEvent struct {

	EventID string `json:"event_id"`

	EventType string `json:"event_type"`

	OccurredAt time.Time `json:"occurred_at"`

	CourseID string `json:"course_id"`

	Title string `json:"title"`

	Description string `json:"description"`

	Category string `json:"category"`

	Instructor string `json:"instructor"`
}