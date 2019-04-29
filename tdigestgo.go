package tdigestgo

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

type centroid struct {
	mean, count float64
}

const locked = math.MinInt64

type Distribution interface {
	ValueAt(q float64) (v float64)
	QuantileOf(v float64) (q float64)
}

// TDigest approximates a distribution of floating point numbers.
type TDigest struct {
	config

	// unmergedIdx is accessed with atomics
	unmergedIdx int64

	centroids centroids

	mu struct {
		sync.RWMutex
		sync.Cond

		// merged is the size of the prefix of centroid used for the merged sorted
		// data.
		numMerged   int
		mergedCount float64
	}
}

func (td *TDigest) Read(f func(d Distribution)) {
	td.merge()
	td.mu.RLock()
	defer td.mu.RUnlock()
	f((*readTDigest)(td))
}

type readTDigest TDigest

func (rtd *readTDigest) ValueAt(q float64) float64 {
	return (*TDigest)(rtd).valueAtRLocked(q)
}

func (rtd *readTDigest) QuantileOf(v float64) float64 {
	panic("not implemented")
}

func New(options ...Option) *TDigest {
	var td TDigest
	defaultOptions.apply(&td.config)
	optionList(options).apply(&td.config)
	td.mu.L = td.mu.RLocker()
	td.centroids = make(centroids, capFromCompression(td.compression))
	return &td
}

func capFromCompression(compression float64) int {
	return (6 * (int)(compression)) + 10
}

func (td *TDigest) Record(mean float64) { td.Add(mean, 1) }

func (td *TDigest) Add(mean, count float64) {
	td.mu.RLock()
	defer td.mu.RUnlock()
	td.centroids[td.getAddIndexRLocked()] = centroid{mean: mean, count: count}
}

func (td *TDigest) Merge(other *TDigest) {
	td.mu.Lock()
	defer td.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	other.mergeLocked()
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
				td.merge()
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
		} else {
			td.mergeLocked()
		}
	}
}

// TODO(ajwerner): optimize reading by storing CDF instead PDF

func (td *TDigest) ValueAt(q float64) (v float64) {
	td.Read(func(d Distribution) {
		v = d.ValueAt(q)
	})
	return v
}

func isVerySmall(v float64) bool {
	return !(v > .000000001 || v < -.000000001)
}

func (td *TDigest) valueAtRLocked(q float64) float64 {
	if q < 0 || q > 1 {
		panic(fmt.Errorf("invalid quantile %v", q))
	}
	// TODO: optimize this to be O(log(merged)) instead of O(merged)
	goal := q * td.mu.mergedCount
	k := 0.0
	i := 0
	for i = 0; i < td.mu.numMerged-1; i++ {
		if k+td.centroids[i].count > goal {
			break
		}
		k += td.centroids[i].count
	}
	n := &td.centroids[i]
	deltaK := goal - k - (n.count / 2)
	if isVerySmall(deltaK) {
		return n.mean
	}
	right := deltaK > 0
	// At the very extremes
	if (right && ((i + 1) == td.mu.numMerged)) || (!right && i == 0) {
		return n.mean
	}
	var nl, nr *centroid
	if right {
		nl = n
		nr = &td.centroids[i+1]
		k += n.count / 2
	} else {
		nl = &td.centroids[i-1]
		nr = n
		k -= n.count / 2
	}
	x := goal - k
	m := (nr.mean - nl.mean) / ((nl.count / 2) + (nr.count / 2))
	return m*x + nl.mean
}

func (td *TDigest) merge() {
	td.mu.Lock()
	defer td.mu.Unlock()
	td.mergeLocked()
}

func assertCountsIncreasing(c centroids) {
	for i := range c[:len(c)] {
		if c[i].count <= c[i+1].count {
			panic(fmt.Errorf("count at %v %v <= %v %v %v", i, c[i], i+1, c[i+1], c))
		}
	}
}

func assertCountsNonZero(c centroids) {
	for i := range c {
		if c[i].count == 0 {
			panic(fmt.Errorf("count at %v %v == 0", i, c[i]))
		}
	}
}

func (td *TDigest) mergeLocked() {
	idx := int(atomic.LoadInt64(&td.unmergedIdx))
	if idx > len(td.centroids) {
		idx = len(td.centroids)
	}
	centroids := td.centroids[:idx]
	sort.Sort(centroids)
	totalCount := 0.0
	for i := 0; i < len(centroids); i++ {
		totalCount += centroids[i].count
	}
	denom := 2 * math.Pi * totalCount * math.Log(totalCount)
	normalizer := td.compression / denom
	cur := 0
	countSoFar := 0.0
	for i := 1; i < len(centroids); i++ {
		proposedCount := centroids[cur].count + centroids[i].count
		z := proposedCount * normalizer
		q0 := countSoFar / totalCount
		q2 := (countSoFar + proposedCount) / totalCount
		shouldAdd := z <= (q0*(1-q0)) && z <= (q2*(1-q2))
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
			cur++
			centroids[cur] = centroids[i]
		}
		if cur != i {
			centroids[i] = centroid{}
		}
	}
	td.mu.numMerged = cur + 1
	td.mu.mergedCount = totalCount
	atomic.StoreInt64(&td.unmergedIdx, int64(td.mu.numMerged))
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

type optionList []Option

func (l optionList) apply(cfg *config) {
	for _, o := range l {
		o.apply(cfg)
	}
}

var defaultOptions = Compression(128)
