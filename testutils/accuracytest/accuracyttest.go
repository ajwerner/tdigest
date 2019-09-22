package accuracytest

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/ajwerner/tdigest"
	"github.com/ajwerner/tdigest/testutils"
)

// AccuracyTest tests the accuracy of a sketch given a distribution.
type AccuracyTest struct {
	name            string
	n               int
	constructorFunc func() tdigest.Sketch
	distribution
	order
	addFunc   func(sketch tdigest.Sketch, data []float64)
	checkFunc func(sketch tdigest.Sketch, data []float64)
}

func (at *AccuracyTest) String() string {
	return at.name + " " + at.distribution.name + " " + at.order.name
}

func (at *AccuracyTest) Run(t testing.TB) {
	if at.addFunc == nil {
		at.addFunc = add
	}
	data := make([]float64, at.n)
	for i := 0; i < at.n; i++ {
		data[i] = at.sample()
	}
	at.order.order(data)
	s := at.constructorFunc()
	at.addFunc(s, data)
	checkAccuracy(t, data, s)
}

var (
	Distributions = []Option{
		Distribution("Uniform", rand.Float64),
		Distribution("Normal", rand.NormFloat64),
		Distribution("Exponential", rand.ExpFloat64),
	}
	Orders = []Option{
		Order("ascending", sort.Float64s),
		Order("descending", func(data []float64) {
			sort.Slice(data, func(i, j int) bool {
				return data[i] > data[j]
			})
		}),
		Order("shuffled", func(data []float64) {
			rand.Shuffle(len(data), func(i, j int) {
				data[i], data[j] = data[j], data[i]
			})
		}),
	}
	ConstructorOps = testutils.CombineOptions(
		[]tdigest.Option{
			tdigest.BufferFactor(1),
			tdigest.BufferFactor(2),
			tdigest.BufferFactor(5),
			tdigest.BufferFactor(10),
			tdigest.BufferFactor(20),
		},
		[]tdigest.Option{
			tdigest.Compression(16),
			tdigest.Compression(64),
			tdigest.Compression(128),
			tdigest.Compression(256),
			tdigest.Compression(512),
		},
		[]tdigest.Option{
			tdigest.UseWeightLimit(true),
			tdigest.UseWeightLimit(false),
		},
	)
	CombinedOptions = CombineOptions(
		[]Option{N(100000)},
		Distributions,
		Orders,
		Constructors(
			"Concurrent",
			func(o ...tdigest.Option) tdigest.Sketch {
				return tdigest.NewConcurrent(o...)
			},
			ConstructorOps),
	)
	Tests = func(testOptions ...[]Option) []AccuracyTest {
		tests := make([]AccuracyTest, len(testOptions))
		for i := range tests {
			optionSlice(testOptions[i]).apply(&tests[i])
		}
		return tests
	}(CombinedOptions...)
)

func Constructors(
	name string, f func(o ...tdigest.Option) tdigest.Sketch, optionSets [][]tdigest.Option,
) []Option {
	out := make([]Option, 0, len(optionSets))
	for i := range optionSets {
		options := optionSets[i]
		constructor := func() tdigest.Sketch {
			return f(options...)
		}
		out = append(out, Options(Constructor(constructor),
			Name(fmt.Sprintf("%v %v", name, options))))
	}
	return out
}

// CombineOptions makes a cartesian product of options
func CombineOptions(options ...[]Option) [][]Option {
	dims := make([]int, 0, len(options))
	for _, o := range options {
		dims = append(dims, len(o))
	}
	var out [][]Option
	create := func(n int) {
		out = make([][]Option, n)
	}
	set := func(outIdx, dim, dimPos int) {
		if len(out[outIdx]) == 0 {
			out[outIdx] = make([]Option, len(options))
		}
		out[outIdx][dim] = options[dim][dimPos]
	}
	testutils.CartesianProduct(create, set, dims...)
	return out
}

func Options(o ...Option) Option { return optionSlice(o) }

type optionSlice []Option

func (o optionSlice) apply(at *AccuracyTest) {
	for _, o := range o {
		o.apply(at)
	}
}

type constructorFunc func() tdigest.Sketch

func Constructor(f func() tdigest.Sketch) Option {
	return constructorFunc(f)
}

func (f constructorFunc) apply(at *AccuracyTest) {
	at.constructorFunc = (func() tdigest.Sketch)(f)
}

func N(n int) Option { return nOption(n) }

type nOption int

func (n nOption) apply(at *AccuracyTest) { at.n = int(n) }

type distribution struct {
	name   string
	sample func() float64
}

func Distribution(name string, sample func() float64) Option {
	return distribution{name: name, sample: sample}
}

func (d distribution) apply(at *AccuracyTest) {
	at.distribution = d
}

func Order(name string, orderFunc func([]float64)) Option {
	return order{name: name, order: orderFunc}
}

type order struct {
	name  string
	order func([]float64)
}

func (o order) apply(at *AccuracyTest) {
	at.order = o
}

type Option interface {
	apply(*AccuracyTest)
}

func Name(n string) Option { return nameOption(n) }

// nameOption adds the provided string to the end of the current test name
type nameOption string

func (n nameOption) apply(at *AccuracyTest) { at.name += string(n) }

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
const defaultCompressionSize = 128

func checkAccuracy(t testing.TB, data []float64, h tdigest.Sketch) {
	sort.Float64s(data)
	N := float64(len(data))
	compressionSize := defaultCompressionSize
	if cs, ok := h.(tdigest.CompressionSizer); ok {
		compressionSize = cs.CompressionSize()
	}
	// check accurracy
	var prevValueAt float64
	for i, q := range quantilesToCheck {
		v := data[int((N-1)*q)]
		got := h.ValueAt(q)
		if i > 0 && got < prevValueAt {
			t.Errorf("Got a value at %v for %v but got a higher value %v at %v",
				q, got, prevValueAt, quantilesToCheck[i-1])
		}
		prevValueAt = got
		avg := math.Abs((v + got) / 2)
		errRatio := math.Abs(v-got) / avg
		if log {
			t.Logf("%.5f %.5f %.9v %16.9v %v\n", errRatio, q, v, got, h.QuantileOf(v))
		}
		limit := permittedError(float64(compressionSize), q, N)
		if errRatio > limit && avg > .1 && math.Abs(got-avg) > .001 {
			t.Errorf("Got error %v for q %v (%v vs %v)",
				errRatio, q, v, got)
		}
	}
	if log {
		t.Logf("%v\n", h)
	}
}

// permittedError is a bespoke, eye-ball statistics check on the acceptable
// amount of error for a sketch given its compressionSize setting, the
// quantile and the number of data points.
func permittedError(compressionSize, q, n float64) float64 {
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
	limit *= defaultCompressionSize / compressionSize
	return limit
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

func add(h tdigest.Sketch, data []float64) {
	for _, v := range data {
		h.Add(v, 1)
	}
}

func AddData(data []float64, hists ...tdigest.Sketch) {
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
