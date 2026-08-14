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

type Job struct {
	ID          string
	Type        string
	Payload     []byte
	CreatedAt   time.Time
	MaxAttempts int
	Status      Status
	Attempts    int
}

func New(t string, payload []byte) *Job {
	return &Job{
		ID:          uuid.NewString(),
		Type:        t,
		Payload:     payload,
		Status:      StatusPending,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
	}
}
