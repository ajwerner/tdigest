package tdigestgo

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestTDigest(t *testing.T) {
	h := New(Compression(123))
	h.Add(1, 1)
	h.Add(2, 1)
	if val := h.ValueAt(0); val != 1 {
		t.Fatalf("Unexpected quantile of large value: got %v, expected %v",
			val, 1)

	}
}

type accuracyTest func() float64

func makeData(N int, f func() float64) []float64 {
	data := make([]float64, 0, N)
	for i := 0; i < N; i++ {
		data = append(data, f())
	}
	return data
}

func shuffleData(data []float64, f func([]float64)) {
	f(data)
}

func sortReverse(data []float64) {
	sort.Slice(data, func(i, j int) bool {
		return data[i] > data[j]
	})
}

var quantilesToCheck = []float64{
	0,
	0.0001,
	0.001,
	0.01,
	0.1,
	0.2,
	0.3,
	0.4,
	0.5,
	0.6,
	0.7,
	0.8,
	0.9,
	0.99,
	0.999,
	0.9999,
	1,
}

func checkAccuracy(t *testing.T, data []float64, h *TDigest) {
	sort.Float64s(data)
	N := float64(len(data))
	// check accurracy
	for _, q := range quantilesToCheck {
		v := data[int((N-1)*q)]
		got := h.ValueAt(q)
		avg := math.Abs((v + got) / 2)
		errRatio := math.Abs(v-got) / avg
		t.Logf("%.5f %v %v %v\n", errRatio, q, v, got)
		limit := 0.5
		if q < .1 || q > .9 {
			limit = .1
		}
		if errRatio > limit && avg > .1 {
			t.Errorf("Got error %v for q %v (%v vs %v)",
				errRatio, q, v, got)
		}
	}
}

func addData(data []float64, hists ...*TDigest) {
	const concurrency = 1000

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	add := func(h *TDigest, v float64) {
		h.Record(v)
		<-sem
		wg.Done()
	}
	wg.Add(len(data))
	for _, d := range data {
		sem <- struct{}{}
		go add(hists[rand.Intn(len(hists))], d)
	}
	wg.Wait()
}

func TestAccuracy(t *testing.T) {
	const N = 100000
	for dist, f := range map[string]accuracyTest{
		"uniform":     rand.Float64,
		"normal":      rand.NormFloat64,
		"exponential": rand.ExpFloat64,
	} {
		data := makeData(N, f)
		for order, shuffle := range map[string]func([]float64){
			"reverse": sortReverse,
			"sorted":  sort.Float64s,
			"shuffled": func(data []float64) {
				rand.Shuffle(len(data), func(i, j int) {
					data[i], data[j] = data[j], data[i]
				})
			},
		} {
			comp := Compression(128)
			t.Run(dist+" "+order+" single", func(t *testing.T) {
				shuffle(data)
				h := New(comp)
				addData(data, h)
				checkAccuracy(t, data, h)
			})
			t.Run(dist+" "+order+" multi", func(t *testing.T) {
				shuffle(data)
				h1 := New(comp)
				h2 := New(comp)
				h3 := New(comp)
				h4 := New(comp)
				addData(data, h1, h2, h3, h4)
				for _, h := range []*TDigest{h2, h3, h4} {
					h1.Merge(h)
				}
				checkAccuracy(t, data, h1)
			})
		}

	}

}

func benchmarkAddSize(size int) func(*testing.B) {
	return func(b *testing.B) {
		h := New(Compression(float64(size)))
		data := make([]float64, 0, b.N)
		for i := 0; i < b.N; i++ {
			data = append(data, rand.Float64())
		}
		b.ResetTimer()
		for _, v := range data {
			h.Add(v, 1)
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	for _, size := range []int{10, 100, 200, 500, 1000, 5000, 10000, 50000} {
		b.Run(strconv.Itoa(size), benchmarkAddSize(size))
	}
}
