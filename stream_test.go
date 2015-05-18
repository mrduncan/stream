package stream

import (
	"reflect"
	"testing"
)

func TestTopLength(t *testing.T) {
	summary := NewSummary(2)
	summary.Observe("one")
	assertEqual(t, len(summary.Top(10)), 1)

	summary.Observe("two")
	assertEqual(t, len(summary.Top(1)), 1)

	assertEqual(t, len(summary.Top(0)), 0)
}

func TestTopOrderedDescending(t *testing.T) {
	summary := NewSummary(3)
	summary.Observe("once a")
	summary.Observe("once b")
	summary.Observe("twice")
	summary.Observe("twice")
	assertEqual(t, summary.Top(3)[0].Item(), "twice")
	assertEqual(t, summary.Top(3)[1].Item(), "once a")
	assertEqual(t, summary.Top(3)[2].Item(), "once b")
}

func TestExceedCapacity(t *testing.T) {
	summary := NewSummary(1)
	summary.Observe("twice")
	summary.Observe("one")
	summary.Observe("twice")
	assertEqual(t, summary.Top(1)[0].Item(), "twice")
	assertEqual(t, summary.Top(1)[0].Count(), uint64(3))
	assertEqual(t, summary.Top(1)[0].ErrorRate(), uint64(2))
}

func TestObserved(t *testing.T) {
	summary := NewSummary(1)
	assertEqual(t, summary.Observed(), uint64(0))
	summary.Observe("item a")
	assertEqual(t, summary.Observed(), uint64(1))
	summary.Observe("item b")
	summary.Observe("item c")
	assertEqual(t, summary.Observed(), uint64(3))
}

func assertEqual(t *testing.T, actual, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Got %+v but expected %+v", actual, expected)
	}
}
