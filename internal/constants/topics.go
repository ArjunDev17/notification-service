package constants

const (

	// ==========================
	// Kafka Topics
	// ==========================

	CourseCreatedTopic = "course.created"

	CourseCreatedRetryTopic = "course.created.retry"

	CourseCreatedDLQTopic = "course.created.dlq"

	// ==========================
	// Consumer Groups
	// ==========================

	NotificationConsumerGroup = "notification-service"
)