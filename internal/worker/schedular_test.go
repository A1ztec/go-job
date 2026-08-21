package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/A1ztec/go-job/internal/queue"
)

func TestScheduler_Register(t *testing.T) {
	q := queue.NewMemoryQueue(3)
	s := NewScheduler(q)

	before := time.Now()
	s.Register("test", []byte(`{"me":"no"}`), 5*time.Second)

	if len(s.schedules) != 1 {
		t.Fatalf("got %d schedules, want 1", len(s.schedules))
	}

	sched := s.schedules[0]
	if sched.JobType != "test" {
		t.Errorf("got job type %q, want %q", sched.JobType, "test")
	}
	if sched.Interval != 5*time.Second {
		t.Errorf("got interval %v, want %v", sched.Interval, 5*time.Second)
	}
	if sched.NextRun.Before(before) || sched.NextRun.After(before.Add(6*time.Second)) {
		t.Errorf("NextRun %v not within expected range", sched.NextRun)
	}
}

func TestScheduler_TickEnqueuesDueJob(t *testing.T) {
	q := queue.NewMemoryQueue(3)
	s := NewScheduler(q)
	s.Register("test", []byte(`{"me":"no"}`), 5*time.Second)

	s.schedules[0].NextRun = time.Now().Add(-time.Second)

	s.tick(context.Background())

	got, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("expected job to be enqueued, got error: %v", err)
	}
	if got.Type != "test" {
		t.Errorf("got job type %q, want %q", got.Type, "test")
	}

	newNextRun := s.schedules[0].NextRun
	if !newNextRun.After(time.Now()) {
		t.Errorf("expected NextRun to be advanced into the future, got %v", newNextRun)
	}
}

func TestScheduler_TickSkipsNotYetDueJob(t *testing.T) {
	q := queue.NewMemoryQueue(3)
	s := NewScheduler(q)
	s.Register("test", []byte(`{"me":"no"}`), 5*time.Second)

	s.tick(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(ctx)
	if err == nil {
		t.Error("expected no job to be enqueued yet — schedule is not due")
	}
}
func TestScheduler_ConcurrentRegister(t *testing.T) {
	q := queue.NewMemoryQueue(20)
	s := NewScheduler(q)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.Register("test", nil, time.Duration(n)*time.Second)
		}(i)
	}
	wg.Wait()

	if len(s.schedules) != 10 {
		t.Errorf("got %d schedules, want 10", len(s.schedules))
	}
}
