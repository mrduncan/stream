// Package stream implements stream algorithms.
package stream

// Counter represents a counted item in a Summary.
type Counter struct {
	Item      string
	Count     uint64
	ErrorRate uint64
}

func newCounter(item string) *Counter {
	return &Counter{Item: item, Count: 1, ErrorRate: 0}
}

// Summary represents a Stream-Summary data structure as described in "Efficient
// Computation of Frequent and Top-k Elements in Data Streams".
type Summary struct {
	capacity int
	counters []*Counter
	index    map[string]int
}

// NewSummary returns a new Summary with the given max capacity.
func NewSummary(capacity int) *Summary {
	return &Summary{capacity: capacity, counters: []*Counter{}, index: make(map[string]int)}
}

// Top returns the top n Counters in the Summary.  If the Summary contains less
// than n Counters, all Counters in the Summary are returned.
func (s *Summary) Top(n int) []*Counter {
	if n > len(s.counters) {
		return s.counters[0:len(s.counters)]
	}

	return s.counters[0:n]
}

// Observe adds an observation of an item to the Summary.
func (s *Summary) Observe(item string) {
	i, exists := s.index[item]
	if exists {
		s.counters[i].Count++
		for i > 0 && s.counters[i].Count > s.counters[i-1].Count {
			s.promote(i)
			i--
		}
	} else {
		if len(s.counters) < s.capacity {
			s.counters = append(s.counters, newCounter(item))
			s.index[item] = len(s.counters) - 1
		} else {
			minCounter := s.counters[len(s.counters)-1]
			delete(s.index, minCounter.Item)

			counter := newCounter(item)
			counter.Count = minCounter.Count + 1
			counter.ErrorRate = minCounter.Count
			s.counters[len(s.counters)-1] = counter
			s.index[item] = len(s.counters) - 1
		}
	}
}

func (s *Summary) promote(i int) {
	toPromote := s.counters[i]
	toDemote := s.counters[i-1]

	s.counters[i-1] = toPromote
	s.index[toPromote.Item] = i - 1

	s.counters[i] = toDemote
	s.index[toDemote.Item] = i
}
