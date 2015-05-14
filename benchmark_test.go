package stream

import (
	"math/rand"
	"strconv"
	"testing"
)

func BenchmarkObserveSmallCapacity(b *testing.B) {
	benchmarkObserve(b, 100, 500000)
}

func BenchmarkObserveLargeCapacity(b *testing.B) {
	benchmarkObserve(b, 2000, 500000)
}

func benchmarkObserve(b *testing.B, capacity int, distinct uint64) {
	r := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(r, 1.5, 5, distinct)

	items := make(chan string, b.N)
	for i := 0; i < b.N; i++ {
		items <- strconv.FormatUint(zipf.Uint64(), 10)
	}

	summary := NewSummary(capacity)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			summary.Observe(<-items)
		}
	})
}
