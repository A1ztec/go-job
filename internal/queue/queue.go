package queue

import (
	"context"
	"errors"
	"time"

	"github.com/A1ztec/go-job/internal/job"
)

var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueEmpty  = errors.New("queue is empty")
)

type Queue interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Dequeue(ctx context.Context) (*job.Job, error)
	Shutdown(ctx context.Context) error
	ScheduleAfter(ctx context.Context, j *job.Job, at time.Duration) error
	SendToDLQ(ctx context.Context, j *job.Job) error
}
