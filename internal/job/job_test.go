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
