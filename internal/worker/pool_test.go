package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/A1ztec/go-job/internal/queue"
)

func TestPool_ProcessesAllJobs(t *testing.T) {
	var wg sync.WaitGroup
	var count atomic.Int32
	// create q
	q := queue.NewMemoryQueue(6)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r := NewRegistry()
	p := NewPool(q, r, 3)
	handler := job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		count.Add(1)
		return nil
	})
	r.Register("test", handler)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := job.New("test", nil)
			err := q.Enqueue(ctx, j)
			if err != nil {
				t.Error("what happen")
			}
		}(i)
	}
	p.Start(ctx)
	wg.Wait()
	if count.Load() != 10 {
		t.Errorf("was expecting like 10 jobs to be dequeued found %v", count.Load())
	}
}

func TestHandleFailure_ExhaustedRetriesGoesToDLQ(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	r := NewRegistry()
	p := NewPool(q, r, 1)
	ctx := context.Background()

	j := job.New("test", nil, job.WithMaxAttempts(1))
	j.Attempts = 0

	p.handleFailure(ctx, j)

	if j.Attempts != 1 {
		t.Errorf("got attempts %d, want 1", j.Attempts)
	}

	// confirm it did NOT get re-enqueued
	dequeueCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(dequeueCtx)
	if err == nil {
		t.Error("expected job NOT to be re-enqueued after exhausting retries")
	}
}

func TestHandleFailure_InstantRetryReEnqueues(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	r := NewRegistry()
	p := NewPool(q, r, 1)
	ctx := context.Background()

	j := job.New("test", nil, job.WithMaxAttempts(3))

	p.handleFailure(ctx, j)

	if j.Attempts != 1 {
		t.Errorf("got attempts %d, want 1", j.Attempts)
	}

	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("expected job to be re-enqueued, got error: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q, want %q", got.ID, j.ID)
	}
}

func TestHandleFailure_BackoffSchedulesInstead(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	r := NewRegistry()
	p := NewPool(q, r, 1)
	ctx := context.Background()

	j := job.New("test", nil, job.WithMaxAttempts(3), job.WithBackoff(50*time.Millisecond))

	p.handleFailure(ctx, j)

	dequeueCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(dequeueCtx)
	if err == nil {
		t.Error("expected job NOT to be immediately available — it should be scheduled with backoff")
	}

	time.Sleep(100 * time.Millisecond)
	got, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("expected job to appear after backoff elapsed, got error: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q, want %q", got.ID, j.ID)
	}
}
