package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	consumerhandler "github.com/ArjunDev17/notification-service/handler/consumer"

	"github.com/ArjunDev17/notification-service/internal/config"
	"github.com/ArjunDev17/notification-service/internal/database"
	"github.com/ArjunDev17/notification-service/internal/kafka"
	"github.com/ArjunDev17/notification-service/internal/logger"
	dispatcher "github.com/ArjunDev17/notification-service/internal/notification"
	emailnotification "github.com/ArjunDev17/notification-service/internal/notification/email"
	notificationusecase "github.com/ArjunDev17/notification-service/internal/usecase/notification"
	"github.com/ArjunDev17/notification-service/internal/worker"

	"github.com/ArjunDev17/notification-service/repository/postgres"

	"github.com/ArjunDev17/notification-service/internal/uow"
	notificationservice "github.com/ArjunDev17/notification-service/service/notification"
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

	//-------------------------------------------------------
	// PostgreSQL
	//-------------------------------------------------------

	db, err := database.New(
		a.config.Database,
	)
	if err != nil {
		return err
	}
	defer db.Close()

	//-------------------------------------------------------
	// Unit Of Work
	//-------------------------------------------------------

	unitOfWork := uow.NewPostgres(
		db,
	)

	//-------------------------------------------------------
	// Kafka Consumer
	//-------------------------------------------------------

	kafkaConsumer := kafka.NewConsumer(
		a.config.Kafka.Brokers,
		a.config.Kafka.ConsumerGroup,
		a.config.Kafka.CourseCreatedTopic,
	)
	defer kafkaConsumer.Close()

	//-------------------------------------------------------
	// Worker Pool
	//-------------------------------------------------------

	workerPool := worker.NewPool(
		a.logger,
		a.config.Kafka.MaxWorkers,
		100,
	)

	//-------------------------------------------------------
	// Repositories
	//-------------------------------------------------------

	notificationRepository := postgres.NewNotificationRepository(
		db.Pool,
	)

	deliveryRepository := postgres.NewDeliveryRepository(
		db.Pool,
	)

	//-------------------------------------------------------
	// Notification Senders
	//-------------------------------------------------------

	emailSender := emailnotification.NewSender(
		a.logger,
	)

	//-------------------------------------------------------
	// Dispatcher
	//-------------------------------------------------------

	notificationDispatcher := dispatcher.NewDispatcher(
		emailSender,
	)

	//-------------------------------------------------------
	// Notification Service
	//-------------------------------------------------------

	notificationService := notificationservice.NewService(
		notificationRepository,
		deliveryRepository,
		notificationDispatcher,
	)

	//-------------------------------------------------------
	// Use Case
	//-------------------------------------------------------

	processCourseCreatedUseCase :=
		notificationusecase.NewProcessCourseCreatedUseCase(
			unitOfWork,
			notificationService,
		)
	//-------------------------------------------------------
	// Kafka Consumer Handler
	//-------------------------------------------------------

	courseCreatedConsumer :=
		consumerhandler.NewCourseCreatedConsumer(
			kafkaConsumer,
			workerPool,
			processCourseCreatedUseCase,
			a.logger,
		)

	//-------------------------------------------------------
	// Start Consumer
	//-------------------------------------------------------

	if err := courseCreatedConsumer.Start(ctx); err != nil {

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
