package tdigest

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

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
			for _, useWeightLimit := range []bool{true, false} {
				options := []Option{
					Compression(128),
					UseWeightLimit(useWeightLimit),
					BufferFactor(10),
				}
				useString := fmt.Sprintf("_%v", useWeightLimit)
				t.Run(dist+" "+order+" single"+useString, func(t *testing.T) {
					shuffle(data)
					h := NewConcurrent(options...)
					addData(data, h)
					checkAccuracy(t, data, h)
				})
				t.Run(dist+" "+order+" multi"+useString, func(t *testing.T) {
					shuffle(data)
					h1 := NewConcurrent(options...)
					h2 := NewConcurrent(options...)
					h3 := NewConcurrent(options...)
					h4 := NewConcurrent(options...)
					addData(data, h1, h2, h3, h4)
					for _, h := range []*Concurrent{h2, h3, h4} {
						h1.Merge(h)
					}
					checkAccuracy(t, data, h1)
				})
			}
		}
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

const log = true

func checkAccuracy(t *testing.T, data []float64, h *Concurrent) {
	sort.Float64s(data)
	N := float64(len(data))
	// check accurracy
	for _, q := range quantilesToCheck {
		v := data[int((N-1)*q)]
		got := h.ValueAt(q)
		avg := math.Abs((v + got) / 2)
		errRatio := math.Abs(v-got) / avg
		if log {
			t.Logf("%.5f %.5f %.9v %16.9v %v\n", errRatio, q, v, got, h.QuantileOf(got))
		}
		qq := math.Abs(q - .5)
		limit := 2.0
		if qq > .4999 {
			limit = .2
		} else if qq > .49 {
			limit = .3
		} else if qq > .4 {
			limit = .4
		} else if limit > .3 {
			limit = .5
		} else if limit > .2 {
			limit = .8
		}
		if errRatio > limit && avg > .1 && math.Abs(got-avg) > .001 {
			t.Errorf("Got error %v for q %v (%v vs %v)",
				errRatio, q, v, got)
		}
	}
	if log {
		t.Logf("%v\n", h)
	}
}

func addData(data []float64, hists ...*Concurrent) {
	const concurrency = 100
	var wg sync.WaitGroup
	divide(len(data), concurrency, func(start, end int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			add(hists[rand.Intn(len(hists))], data[start:end])
		}()
	})
	wg.Wait()
}

// divide splits up n items into a specified number of parts which vary in size
// by at most 1. For exmple say you have a slice you want split into p parts you
// can write it as:
//
//   data, _ := ioutil.ReadAll(r)
//   parts := make([][]byte, 0, p)
//   divide(len(data), p, func(start, end int) {
//     parts = append(parts, data[start:end])
//   }
//
func divide(n, parts int, f func(start, end int)) {
	start := 0
	for i := 0; i < parts; i++ {
		div := parts - i
		res := (n + (div / 2)) / div
		f(start, start+res)
		start += res
		n -= res
	}
}

func add(h Sketch, data []float64) {
	for _, v := range data {
		h.Add(v, 1)
	}
}
