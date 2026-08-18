package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type RedisQueue struct {
	client *redis.Client
	key    string
}

var _ Queue = (*RedisQueue)(nil)

func NewRedisQueue(client *redis.Client, key string) *RedisQueue {
	return &RedisQueue{
		client: client,
		key:    key,
	}
}

func (rq *RedisQueue) Enqueue(ctx context.Context, j *job.Job) error {
	log.Info().Msg("enter the enqueue for redis queue")
	p, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	err = rq.client.LPush(ctx, rq.key, p).Err()
	if err != nil {
		return fmt.Errorf("push job to queue: %w", err)
	}
	return nil
}

func (rq *RedisQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	log.Info().Msg("enter the dequeue from redis queue")
	var j job.Job
	s, err := rq.client.BRPop(ctx, 0, rq.key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to dqueue job from the queue : %w", err)
	}
	err = json.Unmarshal([]byte(s[1]), &j)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the job just dequeued: %w", err)
	}
	return &j, nil
}

func (rq *RedisQueue) Shutdown(ctx context.Context) error {
	err := rq.client.Close()
	if err != nil {
		return fmt.Errorf("failed to shutDown the connection: %w", err)
	}
	return nil
}

func (rq *RedisQueue) ScheduleAfter(ctx context.Context, j *job.Job, at time.Duration) error {
	absTime := time.Now().Add(at)
	p, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	err = rq.client.ZAdd(ctx, rq.delayedKey(), redis.Z{Member: p, Score: float64(absTime.Unix())}).Err()
	if err != nil {
		return fmt.Errorf("schedule job: %w", err)
	}
	return nil
}

func (rq *RedisQueue) delayedKey() string {
	return rq.key + ":delayed"
}
