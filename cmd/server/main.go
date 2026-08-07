package main

import (
	"log"

	"github.com/ArjunDev17/notification-service/internal/app"
)

func main() {

	application := app.New()

	if err := application.Run(); err != nil {
		log.Fatalf(
			"application terminated: %v",
			err,
		)
	}
}
