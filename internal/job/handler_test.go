package job

import (
	"context"
	"errors"
	"testing"
)

func TestHandler(t *testing.T) {
	h := HandlerFunc(func(ctx context.Context, j *Job) error {
		return errors.New("boom")
	})
	j := New("test", nil)
	err := h.Handle(context.Background(), j)

	if err == nil || err.Error() != "boom" {
		t.Errorf("got %v, want error \"boom\"", err)
	}
}
