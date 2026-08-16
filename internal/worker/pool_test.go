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
