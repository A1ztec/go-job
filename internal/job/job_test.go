package job

import "testing"

func TestNew(t *testing.T) {
	j := New("send_email", []byte(`{"to":"a@b.com"}`))

	if j.ID == "" {
		t.Error("expected ID to be generated")
	}
	if j.Status != StatusPending {
		t.Errorf("got status %q, want %q", j.Status, StatusPending)
	}
}

func TestCanRetry(t *testing.T) {
	cases := []struct {
		name        string
		attempts    int
		maxAttempts int
		want        bool
	}{
		{
			name:        "attempts less than max",
			attempts:    1,
			maxAttempts: 3,
			want:        true,
		},
		{
			name:        "attempts equal to max",
			attempts:    3,
			maxAttempts: 3,
			want:        false,
		},
		{
			name:        "attempts greater than max",
			attempts:    4,
			maxAttempts: 3,
			want:        false,
		},
		{
			name:        "zero attempts",
			attempts:    0,
			maxAttempts: 3,
			want:        true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := &Job{
				MaxAttempts: c.maxAttempts,
				Attempts:    c.attempts,
			}
			got := j.CanRetry()
			if got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}
