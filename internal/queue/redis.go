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

func (rq *RedisQueue) Len(ctx context.Context) (int, error) {
	l, err := rq.client.LLen(ctx, rq.key).Result()
	if err != nil {
		return 0, fmt.Errorf("get queue length: %w", err)
	}
	return int(l), err
}

func (rq *RedisQueue) push(ctx context.Context, key string, j *job.Job) error {
	p, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	err = rq.client.LPush(ctx, key, p).Err()
	if err != nil {
		return fmt.Errorf("push job to queue: %w", err)
	}
	return nil
}

func (rq *RedisQueue) Enqueue(ctx context.Context, j *job.Job) error {
	return rq.push(ctx, rq.key, j)
}

func (rq *RedisQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	var j job.Job
	time.Sleep(4 * time.Second)
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

func (rq *RedisQueue) SendToDLQ(ctx context.Context, j *job.Job) error {
	return rq.push(ctx, rq.dlqKey(), j)
}

func (rq *RedisQueue) dlqKey() string {
	return rq.key + ":dlq"
}

func (rq *RedisQueue) delayedKey() string {
	return rq.key + ":delayed"
}

func (rq *RedisQueue) RunPromoter(ctx context.Context, interval time.Duration) {
	script := redis.NewScript(promoteScript)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := script.Run(ctx, rq.client, []string{rq.delayedKey(), rq.key}, time.Now().Unix()).Err()
			if err != nil {
				log.Error().Err(err).Msg("failed to promote scheduled jobs, will retry next interval")
			}
		case <-ctx.Done():
			return
		}
	}
}

const promoteScript = `
	local due = redis.call('ZRANGEBYSCORE' , KEYS[1] , '-inf' , ARGV[1])
	if #due == 0 then
	 return{}
	end
	for i , job in ipairs(due) do 
	 redis.call('ZREM' , KEYS[1] , job)
	 redis.call('LPUSH' , KEYS[2] , job)
	 end
	 return due
`
