// Package stream implements stream algorithms.
package stream

import (
	"container/list"
	"sync"
)

// Counter represents a counted item in a Summary.
type Counter struct {
	item      string
	count     uint64
	errorRate uint64
}

// Item returns the item being counted.
func (c *Counter) Item() string {
	return c.item
}

// Count returns the count for the counter.
func (c *Counter) Count() uint64 {
	return c.count
}

// ErrorRate returns the error rate for the counter.
func (c *Counter) ErrorRate() uint64 {
	return c.errorRate
}

// Summary represents a Stream-Summary data structure as described in "Efficient
// Computation of Frequent and Top-k Elements in Data Streams".
type Summary struct {
	observed uint64
	capacity int
	list     *list.List
	index    map[string]*list.Element
	rw       sync.RWMutex
}

// NewSummary returns a new Summary with the given max capacity.
func NewSummary(capacity int) *Summary {
	return &Summary{
		capacity: capacity,
		list:     list.New(),
		index:    make(map[string]*list.Element),
	}
}

// Observed returns the total number of observations which have occurred.
func (s *Summary) Observed() uint64 {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.observed
}

// Top returns the top n Counters in the Summary.  If the Summary contains less
// than n Counters, all Counters in the Summary are returned.
func (s *Summary) Top(n int) []*Counter {
	s.rw.RLock()
	defer s.rw.RUnlock()

	el := s.list.Front()
	top := make([]*Counter, 0, min(n, s.list.Len()))
	for i := 0; i < n && el != nil; i++ {
		top = append(top, el.Value.(*Counter))
		el = el.Next()
	}

	return top
}

// Observe adds an observation of an item to the Summary.
func (s *Summary) Observe(item string) {
	s.rw.Lock()

	s.observed++
	el, exists := s.index[item]
	if exists {
		s.incrElement(el)
	} else {
		if s.list.Len() < s.capacity {
			s.append(&Counter{item: item, count: 1})
		} else {
			minCounter := s.deleteBack()
			s.append(&Counter{
				item:      item,
				count:     minCounter.count + 1,
				errorRate: minCounter.count,
			})
		}
	}

	s.rw.Unlock()
}

func (s *Summary) append(counter *Counter) {
	s.index[counter.item] = s.list.PushBack(counter)
}

func (s *Summary) deleteBack() *Counter {
	el := s.list.Back()
	s.list.Remove(el)
	c := el.Value.(*Counter)
	delete(s.index, c.item)
	return c
}

func (s *Summary) incrElement(el *list.Element) {
	counter := el.Value.(*Counter)
	counter.count++

	// This element already has the largest count so it won't get moved.
	if s.list.Front() == el {
		return
	}

	// Starting at the previous element, move this element behind the first
	// element we find which has a higher count.
	moved := false
	for currEl := el.Prev(); currEl != nil; currEl = currEl.Prev() {
		if currEl.Value.(*Counter).count > counter.count {
			s.list.MoveAfter(el, currEl)
			moved = true
			break
		}
	}

	// If we didn't find an element with a higher count then this element must
	// have the highest count.  Move it to the front.
	if !moved {
		s.list.MoveToFront(el)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
