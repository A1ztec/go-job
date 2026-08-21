package worker

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/A1ztec/go-job/internal/metrics"
	"github.com/A1ztec/go-job/internal/queue"
)

type Pool struct {
	queue    queue.Queue
	registry *Registry
	size     int //number of workers
}

func NewPool(queue queue.Queue, registry *Registry, size int) *Pool {
	return &Pool{
		queue:    queue,
		registry: registry,
		size:     size, //number of workers
	}
}

func (p *Pool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.size; i++ {
		wg.Add(1)
		go func(WorkerID int) {
			defer wg.Done()
			p.work(ctx, WorkerID)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) work(ctx context.Context, wId int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		j, err := p.queue.Dequeue(ctx)
		if err != nil {
			return
		}
		handler, ok := p.registry.Get(j.Type)
		if !ok {
			log.Info().Int("worker_id", wId).Str("job_type", j.Type).Msg("no handler for job type")
			continue
		}
		func() {
			metrics.WorkersBusy.Inc()
			defer metrics.WorkersBusy.Dec()
			start := time.Now()
			defer func() {
				metrics.JobDuration.WithLabelValues(j.Type).Observe(time.Since(start).Seconds())
			}()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("worker %d: recovered from panic: %v", wId, r)
					metrics.JobsProcessed.WithLabelValues(j.Type, "panics").Inc()
					j.Attempts = j.MaxAttempts - 1
					p.handleFailure(ctx, j)
				}
			}()
			if err := handler.Handle(ctx, j); err != nil {
				log.Error().Int("worker_id", wId).Str("job_id", j.ID).Err(err).Msg("job failed")
				p.handleFailure(ctx, j)
				metrics.JobsProcessed.WithLabelValues(j.Type, "failure").Inc()
				return
			}
			metrics.JobsProcessed.WithLabelValues(j.Type, "success").Inc()
		}()
	}
}

func (p *Pool) handleFailure(ctx context.Context, j *job.Job) {
	j.Attempts++
	if !j.CanRetry() {
		log.Error().Str("job_id", j.ID).Msg("job exhausted retries, moving to DLQ")
		if err := p.queue.SendToDLQ(ctx, j); err != nil {
			log.Error().Err(err).Str("job_id", j.ID).Msg("failed to send job to DLQ — job may be lost")
		}
		return
	}
	if !(j.BackOff > 0) {
		err := p.queue.Enqueue(ctx, j)
		if err != nil {
			log.Error().Err(err).Msg("failed to re-enqueue job")
			return
		}
		log.Info().Int("remaining_attempts", j.MaxAttempts-j.Attempts).Msg("job re-enqueued for retry")
		return
	}
	if err := p.queue.ScheduleAfter(ctx, j, j.BackOff); err != nil {
		log.Error().Err(err).Msg("failed to schedule job for retry")
		return
	}
	log.Info().Dur("backoff", j.BackOff).Msg("job scheduled for delayed retry")
}
