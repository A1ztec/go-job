package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/A1ztec/go-job/internal/queue"
	"github.com/A1ztec/go-job/internal/worker"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var addr string = os.Getenv("REDIS_ADDR")

func main() {
	if addr == "" {
		addr = "localhost:6379"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	client := initRedisClient()
	q := queue.NewRedisQueue(client, fmt.Sprintf("test-%d", time.Now().UnixNano()))
	r := worker.NewRegistry()
	handler := job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		log.Info().Str("job_id", j.ID).Msg("processed job")
		return nil
	})
	r.Register("test", handler)
	r.Register("cleanup", handler)

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	for i := 0; i < 30; i++ {
	// 		time.Sleep(2 * time.Second)
	// 		j := job.New("test", nil)
	// 		err := q.Enqueue(ctx, j)
	// 		if err != nil {
	// 			log.Error().Err(err).Msg("error happen in enqueue a job")
	// 		}
	// 	}
	// }()
	p := worker.NewPool(q, r, 3)
	scheduler := worker.NewScheduler(q)
	scheduler.Register("cleanup", nil, 10*time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Run(ctx, time.Second)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Start(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		q.RunPromoter(ctx, time.Second)
	}()
	wg.Wait()
	if err := q.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("failed to shutdown queue cleanly")
	}
}

func initRedisClient() *redis.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	client := redis.NewClient(&redis.Options{
		Addr:                  addr,
		Password:              "",
		DB:                    0,
		ContextTimeoutEnabled: true, //makes blocking commands (like BRPop) respect ctx cancellation/timeout —
		// without this, they ignore ctx entirely and only stop via their own
		// Redis-level timeout parameter (or never, if that's set to 0)
	})
	for {
		err := client.Ping(ctx).Err()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			log.Fatal().Err(err).Msgf("could not connect to redis at %s", addr)
		case <-time.After(200 * time.Millisecond):
		}
	}
	return client
}
