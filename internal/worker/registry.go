package worker

import (
	"sync"

	"github.com/A1ztec/go-job/internal/job"
)

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]job.Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]job.Handler),
	}
}

func (r *Registry) Register(jobType string, h job.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (job.Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.handlers[jobType]
	return v, ok
}
