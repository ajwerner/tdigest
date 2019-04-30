package tdigest

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestAccuracy(t *testing.T) {
	const N = 10000
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

const log = false

func checkAccuracy(t *testing.T, data []float64, h *TDigest) {
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
}

func addData(data []float64, hists ...*TDigest) {
	const concurrency = 100

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
