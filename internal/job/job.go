package job

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Options func(*Job)

func WithMaxAttempts(Ma int) Options {
	return func(j *Job) {
		j.MaxAttempts = Ma
	}
}

func WithBackoff(t time.Duration) Options {
	return func(j *Job) {
		j.BackOff = t
	}
}

type Job struct {
	ID          string
	Type        string
	Payload     []byte
	CreatedAt   time.Time
	MaxAttempts int
	Status      Status
	Attempts    int
	BackOff     time.Duration
}

func New(t string, payload []byte, options ...Options) *Job {
	j := &Job{
		ID:          uuid.NewString(),
		Type:        t,
		Payload:     payload,
		Status:      StatusPending,
		MaxAttempts: 0,
		CreatedAt:   time.Now(),
		BackOff:     0,
	}
	for _, o := range options {
		o(j)
	}
	return j
}

func (j *Job) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}
