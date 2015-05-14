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

	data := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		data[i] = strconv.FormatUint(zipf.Uint64(), 10)
	}

	summary := NewSummary(capacity)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		summary.Observe(data[i])
	}
}
