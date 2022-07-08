package stg

import (
	"testing"
	"time"
)

func TestUuidIdFactory(t *testing.T) {
	factory := NewUuidFactory()
	got := factory.New()
	if len(got.String()) < 36 {
		t.Errorf("unexpected uuid %s", got.String())
	}
}

func TestStringUuidIdFactory(t *testing.T) {
	factory := NewStringUuidFactory()
	got := factory.New()
	if len(got) < 36 {
		t.Errorf("unexpected uuid %s", got)
	}
}

func TestNow(t *testing.T) {
	nower := NewNower()

	var notExpect time.Time
	got := nower.Now()

	if got.Equal(notExpect) {
		t.Errorf("expected now to return a time value but got a zero value")
	}
}
