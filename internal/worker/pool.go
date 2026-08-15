package worker

import (
	"context"
	"log"
	"sync"

	"github.com/A1ztec/go-job/internal/queue"
)

type Pool struct {
	queue    queue.Queue
	registry *Registry
	size     int //number of workers
}

func NewPool(queue queue.Queue, registry *Registry, size int) *Pool {
	return &Pool{
		queue:    queue,
		registry: registry,
		size:     size, //number of workers
	}
}

func (p *Pool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.size; i++ {
		wg.Add(1)
		go func(WorkerID int) {
			defer wg.Done()
			p.work(ctx, WorkerID)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) work(ctx context.Context, wId int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		j, err := p.queue.Dequeue(ctx)
		if err != nil {
			return
		}
		handler, ok := p.registry.Get(j.Type)
		if !ok {
			log.Printf("worker %d: no handler for job type %q", wId, j.Type)
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("worker %d: recovered from panic: %v", wId, r)
				}
			}()
			if err := handler.Handle(ctx, j); err != nil {
				log.Printf("worker %d: job %s failed: %v", wId, j.ID, err)
			}
		}()
	}
}
