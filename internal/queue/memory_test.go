package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/A1ztec/go-job/internal/job"
)

func TestMemoryQueue_EnqueueDequeue(t *testing.T) {
	q := NewMemoryQueue(100)
	ctx := context.Background()
	j := job.New("test", []byte(`"test"`))

	err := q.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("boom tesh")
	}
	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("boom test")
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q, want %q", got.ID, j.ID)
	}
}

func TestMemoryQueue_DequeueRespectsContext(t *testing.T) {
	q := NewMemoryQueue(100)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
	defer cancel()

	_, err := q.Dequeue(ctx)

	if err == nil {
		t.Fatalf("i was expecting and error got nil")
	}

}
func TestMemoryQueue_EnqueueAfterShutdown(t *testing.T) {
	q := NewMemoryQueue(100)
	ctx := context.Background()
	j := job.New("test job", nil)

	err := q.Shutdown(ctx)
	if err != nil {
		t.Errorf("failed")
	}

	err = q.Enqueue(ctx, j)

	if !errors.Is(err, ErrQueueClosed) {
		t.Errorf("got %v, want ErrQueueClosed", err)
	}
}

func TestMemoryQueue_Concurrent(t *testing.T) {
	q := NewMemoryQueue(20)
	ctx := context.Background()
	var wg sync.WaitGroup
	var count atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := job.New("test", nil)
			_ = q.Enqueue(ctx, j)
		}(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := q.Dequeue(ctx)
			if err == nil {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 10 {
		t.Errorf("dequeue %v expects 10", count.Load())
	}
}

func TestMemoryQueue_DoubleShutDown(t *testing.T) {
	q := NewMemoryQueue(2)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Shutdown(ctx)
			if err != nil {
				t.Errorf("expecting the q to shudown without errors")
			}
		}()
	}
	wg.Wait()
}

func TestMemoryQueue_SendToDLQ(t *testing.T) {
	q := NewMemoryQueue(10)
	ctx := context.Background()
	j := job.New("test", nil)

	err := q.SendToDLQ(ctx, j)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.dlq) != 1 {
		t.Fatalf("got %d jobs in DLQ, want 1", len(q.dlq))
	}
	if q.dlq[0].ID != j.ID {
		t.Errorf("got job ID %q in DLQ, want %q", q.dlq[0].ID, j.ID)
	}
}

func TestMemoryQueue_SendToDLQ_Concurrent(t *testing.T) {
	q := NewMemoryQueue(10)
	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := job.New("test", nil)
			if err := q.SendToDLQ(ctx, j); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if len(q.dlq) != 10 {
		t.Errorf("got %d jobs in DLQ, want 10", len(q.dlq))
	}
}
