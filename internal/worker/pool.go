package worker

import (
	"context"
	"log/slog"
	"sync"
)

type Pool struct {
	logger *slog.Logger

	workers int

	jobs chan Job

	wg sync.WaitGroup
}

func NewPool(
	logger *slog.Logger,
	workers int,
	queueSize int,
) *Pool {

	return &Pool{
		logger:  logger,
		workers: workers,
		jobs: make(
			chan Job,
			queueSize,
		),
	}
}

func (p *Pool) Start(
	ctx context.Context,
) {

	for i := 0; i < p.workers; i++ {

		p.wg.Add(1)

		go p.worker(
			ctx,
			i+1,
		)
	}
}

func (p *Pool) Submit(
	job Job,
) {

	p.jobs <- job
}

func (p *Pool) Shutdown() {

	close(
		p.jobs,
	)

	p.wg.Wait()
}

func (p *Pool) worker(
	ctx context.Context,
	id int,
) {

	defer p.wg.Done()

	p.logger.Info(
		"worker started",
		"id",
		id,
	)

	for {

		select {

		case <-ctx.Done():

			p.logger.Info(
				"worker stopped",
				"id",
				id,
			)

			return

		case job, ok := <-p.jobs:

			if !ok {

				return
			}

			if err := job.Execute(ctx); err != nil {

				p.logger.Error(
					"job failed",
					"worker",
					id,
					"error",
					err,
				)
			}
		}
	}
}
