package job

import "time"

type job struct {
	ID        int
	Type      string
	Payload   []byte
	CreatedAt time.Time
	Attempts  int
}
