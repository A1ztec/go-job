package queue

import (
	"context"
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
	if err := c.Del(ctx, "test").Err(); err != nil {
		t.Logf("cleanup warning: failed to delete test key: %v", err)
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
