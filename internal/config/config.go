package config

import (
	"os"
	"strings"
)

type AppConfig struct {
	Name string
	Port string
}

type KafkaConfig struct {
	Brokers []string

	ConsumerGroup string

	CourseCreatedTopic string

	MaxWorkers int
}

type Config struct {
	App   AppConfig
	Kafka KafkaConfig
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

		Kafka: KafkaConfig{

			Brokers: strings.Split(

				getEnv(
					"KAFKA_BROKERS",
					"localhost:9092",
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
	}
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