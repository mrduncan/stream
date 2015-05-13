package stream

import (
	"testing"
)

func TestTopZeroIsEmpty(t *testing.T) {
	summary := NewSummary(1)
	assertEqualInt(t, len(summary.Top(0)), 0)
}

func TestTopNotLargerThanCapacity(t *testing.T) {
	summary := NewSummary(1)
	summary.Observe("item")
	assertEqualInt(t, len(summary.Top(2)), 1)
}

func TestTopNotLargerThanN(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("one")
	summary.Observe("two")
	assertEqualInt(t, len(summary.Top(1)), 1)
}

func TestTopOrderedDescending(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("once")
	summary.Observe("twice")
	summary.Observe("twice")
	assertEqualString(t, summary.Top(2)[0].Item, "twice")
	assertEqualString(t, summary.Top(2)[1].Item, "once")
}

func TestTopOrderedDescendingWithJump(t *testing.T) {
	summary := NewSummary(3)
	summary.Observe("once a")
	summary.Observe("once b")
	summary.Observe("twice")
	summary.Observe("twice")
	assertEqualString(t, summary.Top(3)[0].Item, "twice")
}

func TestErrorRateWhenExceedingCapacity(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("zero")
	summary.Observe("one")
	summary.Observe("zero")
	summary.Observe("two")
	summary.Observe("zero")
	assertEqualUint64(t, summary.Top(2)[1].ErrorRate, 1)
}

func TestCountWhenExceedingCapacity(t *testing.T) {
	summary := NewSummary(1)
	summary.Observe("zero")
	summary.Observe("one")
	summary.Observe("zero")
	assertEqualUint64(t, summary.Top(1)[0].Count, 3)
}

func assertEqualString(t *testing.T, actual, expected string) {
	if actual != expected {
		t.Errorf("Got %s but expected %s", actual, expected)
	}
}

func assertEqualInt(t *testing.T, actual, expected int) {
	if actual != expected {
		t.Errorf("Got %d but expected %d", actual, expected)
	}
}

func assertEqualUint64(t *testing.T, actual, expected uint64) {
	if actual != expected {
		t.Errorf("Got %d but expected %d", actual, expected)
	}
}
