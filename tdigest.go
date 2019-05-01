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
// All methods are safe to be called concurrently.
//
// Design
//
// The data structure is designed to maintain most of its state in a single
// slice
// The total in-memory size of a TDigest is
//
//    (1+BufferFactor)*(int(Compression)+1)
//
// The data structure does not allocates memory after its construction.
type TDigest struct {
	scale       scaleFunc
	compression float64

	// unmergedIdx is accessed with atomics.
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
	cfg := defaultConfig
	optionList(options).apply(&cfg)
	var td TDigest
	td.mu.L = td.mu.RLocker()
	td.centroids = make(centroids, cfg.bufferSize())
	td.compression = cfg.compression
	td.scale = cfg.scale
	return &td
}

// capFromCompression uses a fixed size buffer of 6x compression.
// TODO(ajwerner): make this configurable.
func capFromCompression(compression float64) int {
	return (6 * (int)(compression))
}

// Sketch provides read access to a float64 valued distribution by
// quantile or by value.
type Sketch interface {
	TotalCount() float64
	ValueAt(q float64) (v float64)
	QuantileOf(v float64) (q float64)
}

// Read enables clients to perform a number of read operations
func (td *TDigest) Read(f func(d Sketch)) {
	td.compress()
	td.mu.RLock()
	defer td.mu.RUnlock()
	f((*readTDigest)(td))
}

// TotalCount returns the total count that has been added to the TDigest.
func (td *TDigest) TotalCount() (c float64) {
	td.Read(func(d Sketch) { c = d.TotalCount() })
	return c
}

// ValueAt returns the value of the quantile q.
// If q is not in [0, 1], ValueAt will panic.
// An empty TDigest will return 0.
func (td *TDigest) ValueAt(q float64) (v float64) {
	td.Read(func(d Sketch) { v = d.ValueAt(q) })
	return v
}

// QuantileOf returns the estimated quantile at which this value falls in the
// distribution. If the v is smaller than any recorded value 0.0 will be
// returned and if v is larger than any recorded value 1.0 will be returned.
// An empty TDigest will return 0.0 for all values.
func (td *TDigest) QuantileOf(v float64) (q float64) {
	td.Read(func(d Sketch) { q = d.QuantileOf(v) })
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
	return quantileOf(td.centroids[:td.mu.numMerged], v)
}

func (td *TDigest) valueAtRLocked(q float64) float64 {
	if q < 0 || q > 1 {
		panic(fmt.Errorf("invalid quantile %v", q))
	}
	if td.mu.numMerged == 0 {
		return 0
	}
	return valueAt(td.centroids[:td.mu.numMerged], q)
}

func (td *TDigest) totalCountRLocked() float64 {
	if td.mu.numMerged == 0 {
		return 0.0
	}
	return td.centroids[td.mu.numMerged-1].count
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
	td.mu.numMerged = compress(td.centroids[:idx], td.compression, td.scale, td.mu.numMerged)
	atomic.StoreInt64(&td.unmergedIdx, int64(td.mu.numMerged))
}

type readTDigest TDigest

var _ Sketch = (*readTDigest)(nil)

func (rtd *readTDigest) ValueAt(q float64) (v float64) {
	return (*TDigest)(rtd).valueAtRLocked(q)
}

func (rtd *readTDigest) QuantileOf(v float64) (q float64) {
	return (*TDigest)(rtd).quantileOfRLocked(v)
}

func (rtd *readTDigest) TotalCount() (c float64) {
	return (*TDigest)(rtd).totalCountRLocked()
}

func valueAt(merged centroids, q float64) float64 {
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
	if !right && i == 0 || right && (i+1) == len(merged) {
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

func quantileOf(merged centroids, v float64) float64 {
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
	return (k + ((v - nl.mean) / m)) / merged[len(merged)-1].count
}

func compress(
	cl centroids, compression float64, scale scaleFunc, numMerged int,
) (newNumMerged int) {
	totalCount := 0.0
	for i := 0; i < numMerged; i++ {
		cl[i].count -= totalCount
		totalCount += cl[i].count
	}
	for i := numMerged; i < len(cl); i++ {
		totalCount += cl[i].count
	}
	sort.Sort(cl)
	normalizer := scale.normalizer(compression, totalCount)
	cur := 0
	countSoFar := 0.0
	for i := 1; i < len(cl); i++ {
		proposedCount := cl[cur].count + cl[i].count
		q0 := countSoFar / totalCount
		q2 := (countSoFar + proposedCount) / totalCount
		probDensity := math.Min(scale.max(q0, normalizer), scale.max(q2, normalizer))
		limit := totalCount * probDensity
		shouldAdd := proposedCount < limit
		if shouldAdd {
			cl[cur].count += cl[i].count
			delta := cl[i].mean - cl[cur].mean
			if delta > 0 {
				weightedDelta := (delta * cl[i].count) / cl[cur].count
				cl[cur].mean += weightedDelta
			}
		} else {
			countSoFar += cl[cur].count
			cl[cur].count = countSoFar
			cur++
			cl[cur] = cl[i]
		}
		if cur != i {
			cl[i] = centroid{}
		}
	}
	cl[cur].count += countSoFar
	return cur + 1
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
