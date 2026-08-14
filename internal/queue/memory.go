package queue

import (
	"context"
	"sync"

	"github.com/A1ztec/go-job/internal/job"
)

type memoryQueue struct {
	jobs   chan *job.Job
	mu     sync.Mutex
	Closed bool
}

var _ Queue = (*memoryQueue)(nil)

func NewMemoryQueue(bufferSize int) *memoryQueue {
	return &memoryQueue{
		jobs: make(chan *job.Job, bufferSize),
	}
}

func (mq *memoryQueue) Enqueue(ctx context.Context, j *job.Job) error {
	select {
	case mq.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mq *memoryQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	select {
	case j := <-mq.jobs:
		return j, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (mq *memoryQueue) Shutdown(ctx context.Context) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if mq.Closed {
		return nil
	}
	mq.Closed = true
	close(mq.jobs)
	return nil
}
