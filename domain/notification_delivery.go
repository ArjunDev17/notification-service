package domain

import "time"

// DeliveryChannel represents the medium used
// to deliver a notification.
type DeliveryChannel string

const (

	ChannelEmail DeliveryChannel = "EMAIL"

	ChannelSMS DeliveryChannel = "SMS"

	ChannelPush DeliveryChannel = "PUSH"

	ChannelSlack DeliveryChannel = "SLACK"

	ChannelWebhook DeliveryChannel = "WEBHOOK"
)

// DeliveryStatus represents the lifecycle
// of a notification delivery.
type DeliveryStatus string

const (

	DeliveryPending DeliveryStatus = "PENDING"

	DeliverySending DeliveryStatus = "SENDING"

	DeliveryDelivered DeliveryStatus = "DELIVERED"

	DeliveryFailed DeliveryStatus = "FAILED"

	DeliveryRetrying DeliveryStatus = "RETRYING"
)

// NotificationDelivery represents one attempt to
// deliver a notification through a specific channel.
type NotificationDelivery struct {

	ID string

	NotificationID string

	Channel DeliveryChannel

	Recipient string

	Status DeliveryStatus

	RetryCount int

	ErrorMessage *string

	SentAt *time.Time

	DeliveredAt *time.Time

	FailedAt *time.Time
}
func (d *NotificationDelivery) MarkSending() {
	d.Status = DeliverySending
}

func (d *NotificationDelivery) MarkDelivered() {

	now := time.Now()

	d.Status = DeliveryDelivered
	d.DeliveredAt = &now
}

func (d *NotificationDelivery) MarkFailed(
	err string,
) {

	now := time.Now()

	d.Status = DeliveryFailed

	d.ErrorMessage = &err

	d.FailedAt = &now
}

func (d *NotificationDelivery) IncrementRetry() {

	d.RetryCount++

	d.Status = DeliveryRetrying
}