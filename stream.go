// Package stream implements stream algorithms.
package stream

import "sync"

// Counter represents a counted item in a Summary.
type Counter struct {
	Item      string
	Count     uint64
	ErrorRate uint64
}

// Summary represents a Stream-Summary data structure as described in "Efficient
// Computation of Frequent and Top-k Elements in Data Streams".
type Summary struct {
	observed uint64
	capacity int
	counters []*Counter
	index    map[string]int
	rw       sync.RWMutex
}

// NewSummary returns a new Summary with the given max capacity.
func NewSummary(capacity int) *Summary {
	return &Summary{
		capacity: capacity,
		counters: make([]*Counter, 0, capacity),
		index:    make(map[string]int),
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

	if n > len(s.counters) {
		return s.counters[0:len(s.counters)]
	}

	return s.counters[0:n]
}

// Observe adds an observation of an item to the Summary.
func (s *Summary) Observe(item string) {
	s.rw.Lock()
	defer s.rw.Unlock()

	s.observed++

	i, exists := s.index[item]
	if exists {
		s.counters[i].Count++

		// Slide this counter forward in the array to keep it in sorted order.
		for i > 0 && s.counters[i].Count > s.counters[i-1].Count {
			s.swap(i, i-1)
			i--
		}
	} else {
		if len(s.counters) < s.capacity {
			// Add the new counter since the summary is below capacity.
			s.counters = append(s.counters, &Counter{Item: item, Count: 1})
			s.index[item] = len(s.counters) - 1
		} else {
			// Replace the lowest counter with a new counter.
			minCounter := s.counters[len(s.counters)-1]
			delete(s.index, minCounter.Item)

			counter := &Counter{
				Item:      item,
				Count:     minCounter.Count + 1,
				ErrorRate: minCounter.Count,
			}
			s.counters[len(s.counters)-1] = counter
			s.index[item] = len(s.counters) - 1
		}
	}
}

func (s *Summary) swap(i, j int) {
	s.index[s.counters[i].Item] = j
	s.index[s.counters[j].Item] = i
	s.counters[j], s.counters[i] = s.counters[i], s.counters[j]
}
