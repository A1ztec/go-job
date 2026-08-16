package worker

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/A1ztec/go-job/internal/job"
)

func TestRegistry_create(t *testing.T) {
	r := NewRegistry()
	h := job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	})
	r.Register("test", h)
	got, ok := r.Get("test")
	if !ok {
		t.Fatal("i was expecting handler of type test got nil")
	}
	if got == nil {
		t.Error("expected non-nil handler")
	}
}

func TestRegistry_GetMissing(t *testing.T) {
	r := NewRegistry()
	got, ok := r.Get("boom")
	if ok {
		t.Error("expecting not to get handler")
	}
	if got != nil {
		t.Error("expaceting no handler cause nothing registered")
	}
}

func TestRegistry_GetAndRegisterConccurent(t *testing.T) {
	r := NewRegistry()
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		randomIndex := rand.IntN(len(chars))
		wg.Add(1)
		go func(i int, randIn int) {
			c := chars[randIn]
			h := job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
				return nil
			})
			defer wg.Done()
			r.Register(string(c), h)
		}(i, randomIndex)

		wg.Add(1)
		go func(i int, randIn int) {
			defer wg.Done()
			c := chars[randIn]
			_, _ = r.Get(string(c))
		}(i, randomIndex)
	}
	wg.Wait()
}
