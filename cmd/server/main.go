package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	consumerhandler "github.com/ArjunDev17/notification-service/handler/consumer"
	"github.com/ArjunDev17/notification-service/internal/config"
	"github.com/ArjunDev17/notification-service/internal/constants"
	"github.com/ArjunDev17/notification-service/internal/kafka"
	"github.com/ArjunDev17/notification-service/internal/logger"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
)

type Application struct {
	config *config.Config
	logger *slog.Logger
}

func New() *Application {

	cfg := config.Load()

	log := logger.New()

	return &Application{
		config: cfg,
		logger: log,
	}
}

func (a *Application) Run() error {

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()

	a.logger.Info(
		"starting notification service",
		"service",
		a.config.App.Name,
	)

	consumer := kafka.NewConsumer(
		a.config.Kafka.Brokers,
		constants.NotificationConsumerGroup,
		constants.CourseCreatedTopic,
	)

	defer consumer.Close()

	useCase := notificationusecase.NewSendCourseNotificationUseCase(
		a.logger,
	)

	handler := consumerhandler.NewCourseCreatedConsumer(
		consumer,
		useCase,
		a.logger,
	)

	if err := handler.Start(ctx); err != nil {

		a.logger.Error(
			"consumer stopped",
			"error",
			err,
		)

		return err
	}

	a.logger.Info("notification service stopped")

	return nil
}