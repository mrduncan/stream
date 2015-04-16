package stream

import (
	"testing"
)

func TestTopZeroIsEmpty(t *testing.T) {
	summary := NewSummary(1)
	assertEqualInt(t, 0, len(summary.Top(0)))
}

func TestTopNotLargerThanCapacity(t *testing.T) {
	summary := NewSummary(1)
	summary.Observe("item")
	assertEqualInt(t, 1, len(summary.Top(2)))
}

func TestTopNotLargerThanN(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("one")
	summary.Observe("two")
	assertEqualInt(t, 1, len(summary.Top(1)))
}

func TestTopOrderedDescending(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("once")
	summary.Observe("twice")
	summary.Observe("twice")
	assertEqualString(t, "twice", summary.Top(2)[0].Item)
	assertEqualString(t, "once", summary.Top(2)[1].Item)
}

func TestTopOrderedDescendingWithJump(t *testing.T) {
	summary := NewSummary(3)
	summary.Observe("once a")
	summary.Observe("once b")
	summary.Observe("twice")
	summary.Observe("twice")
	assertEqualString(t, "twice", summary.Top(3)[0].Item)
}

func TestErrorRateWhenExceedingCapacity(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("zero")
	summary.Observe("one")
	summary.Observe("zero")
	summary.Observe("two")
	summary.Observe("zero")
	assertEqualUint64(t, 1, summary.Top(2)[1].ErrorRate)
}

func TestCountWhenExceedingCapacity(t *testing.T) {
	summary := NewSummary(1)
	summary.Observe("zero")
	summary.Observe("one")
	summary.Observe("zero")
	assertEqualUint64(t, 3, summary.Top(1)[0].Count)
}

func assertEqualString(t *testing.T, expected, actual string) {
	if actual != expected {
		t.Errorf("Expected %s but was %s", expected, actual)
	}
}

func assertEqualInt(t *testing.T, expected, actual int) {
	if actual != expected {
		t.Errorf("Expected %d but was %d", expected, actual)
	}
}

func assertEqualUint64(t *testing.T, expected, actual uint64) {
	if actual != expected {
		t.Errorf("Expected %d but was %d", expected, actual)
	}
}
