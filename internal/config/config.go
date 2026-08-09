package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type AppConfig struct {
	Name string
	Port string
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

type KafkaConfig struct {
	Brokers []string
	ConsumerGroup string

	CourseCreatedTopic string

	MaxWorkers int
}

type NotificationConfig struct {
	// Maximum retry attempts
	// before marking a delivery as failed.
	MaxRetry int

	// Delay between retries.
	RetryDelaySeconds int
}
type Config struct {
	App          AppConfig
	Database     DatabaseConfig
	Kafka        KafkaConfig
	Notification NotificationConfig
}

func Load() *Config {

	return &Config{

		App: AppConfig{

			Name: getEnv(
				"APP_NAME",
				"notification-service",
			),

			Port: getEnv(
				"APP_PORT",
				"8082",
			),
		},

		Database: DatabaseConfig{

			Host: getEnv(
				"DB_HOST",
				"localhost",
			),

			Port: getEnv(
				"DB_PORT",
				"5433",
			),

			User: getEnv(
				"DB_USER",
				"postgres",
			),

			Password: getEnv(
				"DB_PASSWORD",
				"postgres",
			),

			Name: getEnv(
				"DB_NAME",
				"notification_db",
			),

			SSLMode: getEnv(
				"DB_SSLMODE",
				"disable",
			),
		},

		Kafka: KafkaConfig{

			Brokers: strings.Split(

				getEnv(
					"KAFKA_BROKERS",
					"localhost:29092",
				),

				",",
			),

			ConsumerGroup: getEnv(
				"KAFKA_CONSUMER_GROUP",
				"notification-service",
			),

			CourseCreatedTopic: getEnv(
				"KAFKA_TOPIC_COURSE_CREATED",
				"course.created",
			),

			MaxWorkers: 5,
		},
		Notification: NotificationConfig{

			MaxRetry: getEnvAsInt(
				"NOTIFICATION_MAX_RETRY",
				3,
			),

			RetryDelaySeconds: getEnvAsInt(
				"NOTIFICATION_RETRY_DELAY",
				2,
			),
		},
	}
}

func (d DatabaseConfig) DSN() string {

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		d.Host,
		d.Port,
		d.User,
		d.Password,
		d.Name,
		d.SSLMode,
	)
}

func getEnv(
	key string,
	defaultValue string,
) string {

	value := os.Getenv(key)

	if value == "" {
		return defaultValue
	}

	return value
}

func getEnvAsInt(
	key string,
	defaultValue int,
) int {

	value := os.Getenv(key)

	if value == "" {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return intValue
}
