package logger

import (
	"log/slog"
	"os"
)

// New creates and returns a structured JSON logger.
//
// This logger will be shared across the entire application.
func New() *slog.Logger {

	return slog.New(

		slog.NewJSONHandler(

			os.Stdout,

			&slog.HandlerOptions{

				Level: slog.LevelInfo,

				AddSource: true,
			},
		),
	)
}
