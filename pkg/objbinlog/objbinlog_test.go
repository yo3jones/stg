package objbinlog

import "testing"

func TestNew(t *testing.T) {
	got := New[int](nil, nil, nil)

	if got == nil {
		t.Errorf("expected a bin log storage interface but got nil")
	}
}
