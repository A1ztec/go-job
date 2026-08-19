package queue

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T) (*redis.Client, *RedisQueue) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{
		Addr:                  addr,
		Password:              "",
		DB:                    0,
		ContextTimeoutEnabled: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		err := c.Ping(ctx).Err()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("could not connect to redis at %s: %v", addr, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	q := NewRedisQueue(c, "test")
	return c, q
}

func teardown(t *testing.T, c *redis.Client) {
	t.Helper()
	ctx := context.Background()
	for _, key := range []string{"test", "test:delayed", "test:dlq"} {
		if err := c.Del(ctx, key).Err(); err != nil {
			t.Logf("cleanup warning: failed to delete %s: %v", key, err)
		}
	}
	c.Close()
}

func TestRedisQueue_EnqueueDequeue(t *testing.T) {
	c, q := setup(t)
	defer teardown(t, c)

	ctx := context.Background()
	j := job.New("test", nil)

	err := q.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q, want %q", got.ID, j.ID)
	}
}

func TestRedisQueue_DequeueRespectsContext(t *testing.T) {
	c, q := setup(t)
	defer teardown(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(ctx)
	if err == nil {
		t.Error("expected an error, got nil")
	}
}

func TestRedisQueue_SendToDLQ(t *testing.T) {
	c, q := setup(t)
	defer teardown(t, c)

	ctx := context.Background()
	j := job.New("test", nil)

	err := q.SendToDLQ(ctx, j)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// DLQ uses a different key — drain it directly to confirm the job landed there
	result, err := c.BRPop(ctx, 2*time.Second, q.dlqKey()).Result()
	if err != nil {
		t.Fatalf("expected job in DLQ, got error: %v", err)
	}

	var got job.Job
	if err := json.Unmarshal([]byte(result[1]), &got); err != nil {
		t.Fatalf("failed to decode DLQ job: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q in DLQ, want %q", got.ID, j.ID)
	}
}

func TestRedisQueue_RunPromoter(t *testing.T) {
	c, q := setup(t)
	defer teardown(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	j := job.New("test", nil)
	if err := q.ScheduleAfter(ctx, j, 500*time.Millisecond); err != nil {
		t.Fatalf("unexpected error scheduling job: %v", err)
	}

	// start the promoter with a fast interval so the test doesn't take long
	go q.RunPromoter(ctx, 100*time.Millisecond)

	// job should NOT be available immediately
	tooSoonCtx, cancelTooSoon := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelTooSoon()
	_, err := q.Dequeue(tooSoonCtx)
	if err == nil {
		t.Error("expected job NOT to be available before its scheduled time")
	}

	// after the schedule time + at least one promoter tick, it should show up
	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("expected job to be promoted and dequeued, got error: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("got job ID %q, want %q", got.ID, j.ID)
	}
}
