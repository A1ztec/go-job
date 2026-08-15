package queue

import (
	"context"
	"testing"

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
