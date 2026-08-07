package mapper

import (
	"time"

	"github.com/google/uuid"

	"github.com/ArjunDev17/notification-service/domain"
	"github.com/ArjunDev17/notification-service/events"
)

type NotificationBundle struct {
	Notification *domain.Notification
	Deliveries   []*domain.NotificationDelivery
}

// ToNotificationBundle converts a Kafka CourseCreatedEvent
// into domain entities.
func ToNotificationBundle(
	event events.CourseCreatedEvent,
) *NotificationBundle {

	notificationID := uuid.NewString()

	notification := &domain.Notification{

		ID: notificationID,

		Type: event.EventType,

		Title: event.Title,

		Message: event.Description,

		// Event-specific information.
		Metadata: map[string]any{

			"course_id": event.CourseID,

			"title": event.Title,

			"description": event.Description,

			"category": event.Category,

			"instructor": event.Instructor,

			"event_type": event.EventType,
		},

		CreatedAt: time.Now(),
	}

	deliveries := []*domain.NotificationDelivery{

		{
			ID: uuid.NewString(),

			NotificationID: notificationID,

			Channel: domain.ChannelEmail,

			Recipient: "arjun@gmail.com",

			Status: domain.DeliveryPending,
		},
	}

	return &NotificationBundle{

		Notification: notification,

		Deliveries: deliveries,
	}
}
