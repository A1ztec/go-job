package queue

import (
	"context"

	"github.com/A1ztec/go-job/internal/job"
)

type Queue interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Dequeue(ctx context.Context) (*job.Job, error)
	Shutdown(ctx context.Context) error
}
