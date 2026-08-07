package notification

import (
	"context"
	"fmt"

	"github.com/ArjunDev17/notification-service/domain"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
)

// Dispatcher routes notifications
// to the correct Sender implementation.
type Dispatcher struct {

	// Registered senders by channel.
	senders map[domain.DeliveryChannel]notificationservice.Sender
}

// NewDispatcher registers all available senders.
func NewDispatcher(
	senders ...notificationservice.Sender,
) *Dispatcher {

	dispatcher := &Dispatcher{
		senders: make(
			map[domain.DeliveryChannel]notificationservice.Sender,
		),
	}

	for _, sender := range senders {

		dispatcher.senders[sender.Channel()] = sender
	}

	return dispatcher
}

// Dispatch sends the notification using
// the sender that matches the delivery channel.
func (d *Dispatcher) Dispatch(
	ctx context.Context,
	delivery *domain.NotificationDelivery,
	notification *domain.Notification,
) error {

	sender, exists := d.senders[delivery.Channel]

	if !exists {

		return fmt.Errorf(
			"no sender registered for channel %s",
			delivery.Channel,
		)
	}

	return sender.Send(
		ctx,
		delivery,
		notification,
	)
}