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

	data := []string{}
	for i := 0; i < b.N; i++ {
		item := strconv.FormatUint(zipf.Uint64(), 10)
		data = append(data, item)
	}

	summary := NewSummary(capacity)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		summary.Observe(data[i])
	}
}
