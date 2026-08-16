package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/A1ztec/go-job/internal/job"
	"github.com/A1ztec/go-job/internal/queue"
	"github.com/A1ztec/go-job/internal/worker"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	q := queue.NewMemoryQueue(3)
	r := worker.NewRegistry()
	handler := job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		log.Info().Str("job_id", j.ID).Msg("processed job")
		return nil
	})
	r.Register("test", handler)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			time.Sleep(2 * time.Second)
			j := job.New("test", nil)
			err := q.Enqueue(ctx, j)
			if err != nil {
				log.Error().Err(err).Msg("error happen in enqueue a job")
			}
		}
	}()
	p := worker.NewPool(q, r, 3)
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Start(ctx)
	}()
	wg.Wait()
}
