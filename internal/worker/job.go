package worker

import "context"

// Job represents one unit of work.
type Job interface {
	Execute(
		ctx context.Context,
	) error
}

// Result is returned by a worker
// after processing one job.
type Result struct {
	Err error
}
