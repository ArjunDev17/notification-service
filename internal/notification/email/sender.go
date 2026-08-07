package email

import (
	"context"
	"log/slog"

	"github.com/ArjunDev17/notification-service/domain"
)

// Sender delivers notifications over Email.
type Sender struct {
	logger *slog.Logger
}

// NewSender creates a new Email sender.
func NewSender(
	logger *slog.Logger,
) *Sender {

	return &Sender{
		logger: logger,
	}
}

// Channel tells Dispatcher which notification
// channel this sender supports.
func (s *Sender) Channel() domain.DeliveryChannel {

	return domain.ChannelEmail
}

// Send delivers an email notification.
func (s *Sender) Send(
	ctx context.Context,
	delivery *domain.NotificationDelivery,
	notification *domain.Notification,
) error {

	// ------------------------------------------------------------------
	// For now we're simulating email sending.
	// Later we'll integrate:
	//
	// ✓ AWS SES
	// ✓ SendGrid
	// ✓ Mailgun
	// ✓ SMTP
	// ------------------------------------------------------------------

	s.logger.Info(
		"sending email notification",

		"notification_id", notification.ID,

		"recipient", delivery.Recipient,

		"title", notification.Title,

		"channel", delivery.Channel,
	)

	// TODO:
	// smtp.Send(...)
	// ses.Send(...)
	// sendgrid.Send(...)

	return nil
}