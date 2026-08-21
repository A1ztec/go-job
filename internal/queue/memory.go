package queue

import (
	"context"
	"sync"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/rs/zerolog/log"
)

type memoryQueue struct {
	jobs   chan *job.Job
	mu     sync.Mutex
	dlq    []*job.Job
	dlqMu  sync.Mutex
	closed bool
}

var _ Queue = (*memoryQueue)(nil)

func NewMemoryQueue(bufferSize int) *memoryQueue {
	return &memoryQueue{
		jobs: make(chan *job.Job, bufferSize),
	}
}

func (mq *memoryQueue) Len(ctx context.Context) (int, error) {
	return len(mq.jobs), nil
}

func (mq *memoryQueue) Enqueue(ctx context.Context, j *job.Job) error {
	mq.mu.Lock()
	if mq.closed {
		mq.mu.Unlock()
		return ErrQueueClosed
	}
	mq.mu.Unlock()
	select {
	case mq.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mq *memoryQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	select {
	case j, ok := <-mq.jobs:
		if !ok {
			return nil, ErrQueueClosed
		}
		return j, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (mq *memoryQueue) Shutdown(ctx context.Context) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if mq.closed {
		return nil
	}
	close(mq.jobs)
	mq.closed = true
	return nil
}

func (mq *memoryQueue) ScheduleAfter(ctx context.Context, j *job.Job, d time.Duration) error {
	time.AfterFunc(d, func() {
		enqueueCtx := context.Background()
		if err := mq.Enqueue(enqueueCtx, j); err != nil {
			log.Error().Err(err).Str("job_id", j.ID).Msg("failed to enqueue delayed job")
		}
	})
	return nil
}

func (mq *memoryQueue) SendToDLQ(ctx context.Context, j *job.Job) error {
	mq.dlqMu.Lock()
	defer mq.dlqMu.Unlock()
	mq.dlq = append(mq.dlq, j)
	return nil
}
