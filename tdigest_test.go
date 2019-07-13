package tdigest

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func TestTDigest(t *testing.T) {
	td := NewConcurrent(Compression(123))
	if td.ValueAt(1) != 0 {
		t.Fatalf("failed to retreive value from empty TDigest, %v", td.ValueAt(1))
	}
	td.Add(1, 1)
	td.Add(2, 1)
	if val := td.ValueAt(0); val != 1 {
		t.Fatalf("Unexpected quantile of large value: got %v, expected %v",
			val, 1)
	}
}

func BenchmarkAdd(b *testing.B) {
	for _, size := range []int{10, 100, 200, 500, 1000, 5000, 10000, 50000} {
		newSketch := func() Sketch { return New(Compression(float64(size))) }
		b.Run(strconv.Itoa(size), benchmarkAddSize(newSketch, size))
	}
}

func BenchmarkConcurrent(b *testing.B) {
	for _, readPercent := range []float64{.0001, .001, .01, .1, 1, 10, 50} {
		for _, concurrency := range []int{1, 2, 4, 8, 16, 32, 64} {
			for _, size := range []int{10, 100, 200 /*500, 1000, 5000, 10000, 50000*/} {
				newSketch := func() Sketch {
					return NewConcurrent(Compression(float64(size)))
				}
				b.Run(fmt.Sprintf("buf_size=%d,concurrency=%d,reads=%f",
					size, concurrency, readPercent),
					benchmarkConcurrentAddSize(newSketch, size, concurrency, readPercent))
			}
		}
	}
}

func benchmarkConcurrentAddSize(
	newSketch func() Sketch, size, concurrency int, readPercent float64,
) func(*testing.B) {
	return func(b *testing.B) {
		h := newSketch()
		data := make([]float64, 0, b.N)
		for i := 0; i < b.N; i++ {
			data = append(data, rand.Float64())
		}
		var wg sync.WaitGroup
		wg.Add(concurrency)
		readFrac := readPercent / 100

		b.ResetTimer()
		for goroutine := 0; goroutine < concurrency; goroutine++ {
			go func(goroutine int) {
				for i := goroutine; i < b.N; i += concurrency {
					h.Add(data[i], 1)
					if data[i] < readFrac {
						h.ValueAt(.2)
					}
				}
				wg.Done()
			}(goroutine)
		}
		wg.Wait()
		b.SetBytes(8)
	}
}

func benchmarkAddSize(newSketch func() Sketch, size int) func(*testing.B) {
	return func(b *testing.B) {
		h := newSketch()
		data := make([]float64, 0, b.N)
		for i := 0; i < b.N; i++ {
			data = append(data, rand.Float64())
		}
		b.ResetTimer()
		for _, v := range data {
			h.Add(v, 1)
		}
		b.SetBytes(8)
	}
}
