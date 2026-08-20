package worker

import (
	"context"
	"sync"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/A1ztec/go-job/internal/queue"
	"github.com/rs/zerolog/log"
)

type Schedule struct {
	JobType  string
	Payload  []byte
	Interval time.Duration
	NextRun  time.Time
}

type Scheduler struct {
	mu        sync.RWMutex
	schedules []Schedule
	queue     queue.Queue
}

func NewScheduler(q queue.Queue) *Scheduler {
	return &Scheduler{
		queue: q,
	}
}

func (s *Scheduler) Register(jobType string, payload []byte, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sc := Schedule{
		JobType:  jobType,
		Payload:  payload,
		Interval: interval,
		NextRun:  time.Now().Add(interval),
	}
	s.schedules = append(s.schedules, sc)
}

func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for i := range s.schedules {
		sched := &s.schedules[i]
		if now.Before(sched.NextRun) {
			continue
		}
		j := job.New(sched.JobType, sched.Payload)
		if err := s.queue.Enqueue(ctx, j); err != nil {
			log.Error().Err(err).Str("job_type", sched.JobType).Msg("failed to enqueue scheduled job")
			continue
		}

		sched.NextRun = now.Add(sched.Interval)
	}
}
