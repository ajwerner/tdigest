// Package tdigest provides a concurrent, streaming quantiles estimation data
// structure for float64 data.
package tdigest

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

// TDigest approximates a distribution of floating point numbers.
type TDigest struct {
	config

	// unmergedIdx is accessed with atomics
	unmergedIdx int64

	centroids centroids

	mu struct {
		sync.RWMutex
		sync.Cond

		// numMerged is the size of the prefix of centroid used for the merged
		// sorted data.
		numMerged int
	}
}

// New creates a new TDigest.
func New(options ...Option) *TDigest {
	var td TDigest
	defaultOptions.apply(&td.config)
	optionList(options).apply(&td.config)
	td.mu.L = td.mu.RLocker()
	td.centroids = make(centroids, capFromCompression(td.compression))
	return &td
}

// capFromCompression uses a fixed size buffer of 6x compression.
// TODO(ajwerner): make this configurable.
func capFromCompression(compression float64) int {
	return (6 * (int)(compression))
}

// Distribution provides read access to a float64 valued distribution by
// quantile or by value.
type Distribution interface {
	ValueAt(q float64) (v float64)
	QuantileOf(v float64) (q float64)
}

// Read enables clients to perform a number of read operations
func (td *TDigest) Read(f func(d Distribution)) {
	td.compress()
	td.mu.RLock()
	defer td.mu.RUnlock()
	f((*readTDigest)(td))
}

// ValueAt returns the value of the quantile q.
// If q is not in [0, 1], ValueAt will panic.
// An empty TDigest will return 0.
func (td *TDigest) ValueAt(q float64) (v float64) {
	td.Read(func(d Distribution) { v = d.ValueAt(q) })
	return v
}

// QuantileOf returns the estimated quantile at which this value falls in the
// distribution. If the v is smaller than any recorded value 0.0 will be
// returned and if v is larger than any recorded value 1.0 will be returned.
// An empty TDigest will return 0.0 for all values.
func (td *TDigest) QuantileOf(v float64) (q float64) {
	td.Read(func(d Distribution) { q = d.QuantileOf(v) })
	return q
}

type centroid struct {
	mean, count float64
}

// Add adds the provided data to the TDigest.
func (td *TDigest) Add(mean, count float64) {
	td.mu.RLock()
	defer td.mu.RUnlock()
	td.centroids[td.getAddIndexRLocked()] = centroid{mean: mean, count: count}
}

// Record records adds a value with a count of 1.
func (td *TDigest) Record(mean float64) { td.Add(mean, 1) }

// Merge combines other into td.
func (td *TDigest) Merge(other *TDigest) {
	td.mu.Lock()
	defer td.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	other.compressLocked()
	totalCount := 0.0
	for i := range other.centroids {
		other.centroids[i].count -= totalCount
		totalCount += other.centroids[i].count
	}
	perm := rand.Perm(other.mu.numMerged)
	for _, i := range perm {
		td.centroids[td.getAddIndexLocked()] = other.centroids[i]
	}
}

func (td *TDigest) getAddIndexRLocked() (r int) {
	for {
		idx := int(atomic.AddInt64(&td.unmergedIdx, 1))
		idx--
		if idx < len(td.centroids) {
			return idx
		} else if idx == len(td.centroids) {
			func() {
				td.mu.RUnlock()
				defer td.mu.RLock()
				td.compress()
				td.mu.Broadcast()
			}()
		} else {
			td.mu.Wait()
		}
	}
}

func (td *TDigest) getAddIndexLocked() int {
	for {
		idx := int(atomic.AddInt64(&td.unmergedIdx, 1)) - 1
		if idx < len(td.centroids) {
			return idx
		}
		td.compressLocked()
	}
}

func (td *TDigest) quantileOfRLocked(v float64) float64 {
	if td.mu.numMerged == 0 {
		return 0
	}
	merged := td.centroids[:td.mu.numMerged]
	i := sort.Search(len(merged), func(i int) bool {
		return merged[i].mean >= v
	})
	// Deal with the ends of the distribution.
	if i == 0 {
		return 0
	}
	if i+1 == len(merged) && v >= merged[i].mean {
		return 1
	}
	k := merged[i-1].count
	nr := merged[i]
	nr.count -= k
	nl := merged[i-1]
	if i > 1 {
		nl.count -= merged[i-2].count
	}

	delta := (nr.mean - nl.mean)
	cost := ((nl.count / 2) + (nr.count / 2))
	m := delta / cost
	return (k + ((v - nl.mean) / m)) / merged[td.mu.numMerged-1].count
}

func (td *TDigest) valueAtRLocked(q float64) float64 {
	if q < 0 || q > 1 {
		panic(fmt.Errorf("invalid quantile %v", q))
	}
	if td.mu.numMerged == 0 {
		return 0
	}
	merged := td.centroids[:td.mu.numMerged]
	goal := q * merged[len(merged)-1].count
	i := sort.Search(len(merged), func(i int) bool {
		return merged[i].count >= goal
	})
	n := merged[i]
	k := 0.0
	if i > 0 {
		k = merged[i-1].count
		n.count -= merged[i-1].count
	}
	deltaK := goal - k - (n.count / 2)
	right := deltaK > 0

	// if before the first point or after the last point, return the current mean.
	if !right && i == 0 || right && (i+1) == td.mu.numMerged {
		return n.mean
	}
	var nl, nr centroid
	if right {
		nl = n
		nr = merged[i+1]
		nr.count -= merged[i].count
		k += nl.count / 2
	} else {
		nl = merged[i-1]
		if i > 1 {
			nl.count -= merged[i-2].count
		}
		nr = n
		k -= nr.count / 2
	}
	x := goal - k
	m := (nr.mean - nl.mean) / ((nl.count / 2) + (nr.count / 2))
	return m*x + nl.mean
}

type readTDigest TDigest

var _ Distribution = (*readTDigest)(nil)

func (rtd *readTDigest) ValueAt(q float64) (v float64) {
	return (*TDigest)(rtd).valueAtRLocked(q)
}

func (rtd *readTDigest) QuantileOf(v float64) (q float64) {
	return (*TDigest)(rtd).quantileOfRLocked(v)
}

func (td *TDigest) compress() {
	td.mu.Lock()
	defer td.mu.Unlock()
	td.compressLocked()
}

func (td *TDigest) compressLocked() {
	idx := int(atomic.LoadInt64(&td.unmergedIdx))
	if idx > len(td.centroids) {
		idx = len(td.centroids)
	}
	centroids := td.centroids[:idx]
	totalCount := 0.0
	for i := 0; i < td.mu.numMerged; i++ {
		centroids[i].count -= totalCount
		totalCount += centroids[i].count
	}
	for i := td.mu.numMerged; i < len(centroids); i++ {
		totalCount += centroids[i].count
	}
	sort.Sort(centroids)
	normalizer := td.scale.normalizer(td.compression, totalCount)
	cur := 0
	countSoFar := 0.0
	for i := 1; i < len(centroids); i++ {
		proposedCount := centroids[cur].count + centroids[i].count
		q0 := countSoFar / totalCount
		q2 := (countSoFar + proposedCount) / totalCount
		limit := totalCount * math.Min(td.scale.max(q0, normalizer), td.scale.max(q2, normalizer))
		shouldAdd := proposedCount < limit
		if shouldAdd {
			centroids[cur].count += centroids[i].count
			delta := centroids[i].mean - centroids[cur].mean
			if delta > 0 {
				weightedDelta := (delta * centroids[i].count) /
					centroids[cur].count
				centroids[cur].mean += weightedDelta
			}
		} else {
			countSoFar += centroids[cur].count
			centroids[cur].count = countSoFar
			cur++
			centroids[cur] = centroids[i]
		}
		if cur != i {
			centroids[i] = centroid{}
		}
	}
	centroids[cur].count += countSoFar
	td.mu.numMerged = cur + 1
	atomic.StoreInt64(&td.unmergedIdx, int64(td.mu.numMerged))
}

type scaleFunc int

const (
	// k2 is the default scaleFunc.
	k2 scaleFunc = iota
)

var scaleFuncs = []struct {
	normalizer func(compression, totalCount float64) float64
	k          func(q, normalizer float64) float64
	q          func(k, normalizer float64) float64
	max        func(q, normalizer float64) float64
}{
	k2: {
		normalizer: func(compression, totalCount float64) float64 {
			return compression / (4*math.Log(totalCount/compression) + 24)
		},
		k: func(q, normalizer float64) float64 {
			if q < 1e-15 {
				q = 1e-15
			} else if q > 1-1e15 {
				q = 1 - 1e15
			}

			fmt.Println(math.Log(1/(1-q)) * normalizer)
			return math.Log(1/(1-q)) * normalizer
		},
		q: func(k, normalizer float64) float64 {
			w := math.Exp(k / normalizer)
			return w / (1 + w)
		},
		max: func(q, normalizer float64) float64 {
			return q * (1 - q) / normalizer
		},
	},
}

func (sf scaleFunc) normalizer(compression, totalCount float64) float64 {
	return scaleFuncs[sf].normalizer(compression, totalCount)
}

func (sf scaleFunc) k(q, normalizer float64) float64 {
	return scaleFuncs[sf].k(q, normalizer)
}

func (sf scaleFunc) q(k, normalizer float64) float64 {
	return scaleFuncs[sf].q(k, normalizer)
}

func (sf scaleFunc) max(k, normalizer float64) float64 {
	return scaleFuncs[sf].max(k, normalizer)
}

type centroids []centroid

var _ sort.Interface = centroids(nil)

func (c centroids) totalCount() float64 {
	var sum float64
	for i := range c {
		sum += c[i].count
	}
	return sum
}

func (c centroids) Len() int           { return len(c) }
func (c centroids) Less(i, j int) bool { return c[i].mean < c[j].mean }
func (c centroids) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type config struct {
	compression float64
	scale       scaleFunc
}

type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(cfg *config) { f(cfg) }

func Compression(compression float64) Option {
	return optionFunc(func(cfg *config) {
		cfg.compression = compression
	})
}

func scale(sf scaleFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.scale = sf
	})
}

type optionList []Option

func (l optionList) apply(cfg *config) {
	for _, o := range l {
		o.apply(cfg)
	}
}

var defaultOptions = optionList{
	Compression(128),
	scale(k2),
}
