package tdigest

import (
	"fmt"
	"math"

	"github.com/ajwerner/tdigest/internal/scale"
	"github.com/ajwerner/tdigest/internal/tdigest"
)

// Sketch is an
type Sketch interface {
	Reader
	Recorder
}

// Recorder is the write interface to a Sketch.
type Recorder interface {
	Add(mean, count float64)
}

// Reader provides read access to a float64 valued distribution by
// quantile or by value.
type Reader interface {
	InnerMean(innerQ float64) float64
	TrimmedMean(lo, hi float64) float64
	TotalCount() float64
	TotalSum() float64
	ValueAt(q float64) (v float64)
	QuantileOf(v float64) (q float64)
}

// AddToer allows a sketch to be added to another.
type AddToer interface {
	AddTo(Recorder)
}

// CompressionSizer is an interface to expose the Compression factor for a tdigest.
type CompressionSizer interface {
	// CompressionSize is the maximum number of centroids which a TDigest will
	// store when fully compressed. If the TDigest is using the weightLimit
	// heuristic then this is a target, not an upper bound.
	CompressionSize() int
}

type TDigest struct {
	scale          scale.Func
	compression    float64
	useWeightLimit bool

	centroids   []tdigest.Centroid
	numMerged   int
	unmergedIdx int
}

// New creates a new Concurrent.
func New(options ...Option) *TDigest {
	cfg := defaultConfig
	optionList(options).apply(&cfg)
	var td TDigest
	td.centroids = make([]tdigest.Centroid, cfg.bufferSize())
	td.compression = cfg.compression
	td.scale = cfg.scale
	td.useWeightLimit = cfg.useWeightLimit
	return &td
}

// Add adds data to the TDigest with the provided mean and count.
func (td *TDigest) Add(mean, count float64) {
	if td.unmergedIdx == len(td.centroids) {
		td.compress()
	}
	td.centroids[td.unmergedIdx] = tdigest.Centroid{
		Mean:  mean,
		Count: count,
	}
	td.unmergedIdx++
}

// Record is a shorthand for td.Add(mean, 1).
func (td *TDigest) Record(mean float64) {
	td.Add(mean, 1)
}

// CompressionSize is the maximum number of centroids which a TDigest will
// store when fully compressed.
func (td *TDigest) CompressionSize() int {
	return int(math.Ceil(td.compression))
}

// AddTo adds the data from td into the provided Recorder.
func (td *TDigest) AddTo(into Recorder) {
	td.compress()
	addTo(into, td.centroids[:td.numMerged])
}

// TotalCount returns the total amount of count which has been added to td.
// It requires flushing the buffer then is an O(1) operation.
func (td *TDigest) TotalCount() (c float64) {
	td.compress()
	return tdigest.TotalCount(td.centroids[:td.numMerged])
}

// InnerMean returns the mean of the inner quantile range.
// It requires flushing the buffer then is an O(n) operation.
func (td *TDigest) InnerMean(inner float64) (c float64) {
	td.compress()
	lo := inner / 2
	return tdigest.TrimmedMean(td.centroids[:td.numMerged], lo, 1-lo)
}

// TrimmedMean returns the mean of values which lie in the quantile range
// between lo and hi.
// It requires flushing the buffer then is an O(n) operation.
func (td *TDigest) TrimmedMean(lo, hi float64) (c float64) {
	td.compress()
	return tdigest.TrimmedMean(td.centroids[:td.numMerged], lo, hi)
}

// TotalSum returns the total amount of data added to the TDigest weighted by
// its associated count.
func (td *TDigest) TotalSum() float64 {
	td.compress()
	return tdigest.TotalSum(td.centroids[:td.numMerged])
}

func (td *TDigest) String() string {
	return readerString(td)
}

func (td *TDigest) Clear() {
	*td = TDigest{
		scale:          td.scale,
		compression:    td.compression,
		useWeightLimit: td.useWeightLimit,
		centroids:      td.centroids,
	}
}

func (td *TDigest) ValueAt(q float64) (v float64) {
	td.compress()
	return tdigest.ValueAt(td.centroids[:td.numMerged], q)
}

// QuantileOf returns the estimated quantile at which this value falls in the
// distribution. If the v is smaller than any recorded value 0.0 will be
// returned and if v is larger than any recorded value 1.0 will be returned.
// An empty Concurrent will return 0.0 for all values.
func (td *TDigest) QuantileOf(v float64) (q float64) {
	td.compress()
	return tdigest.QuantileOf(td.centroids[:td.numMerged], v)
}

func (td *TDigest) compress() {
	td.numMerged = tdigest.Compress(td.centroids[:td.unmergedIdx], td.compression, td.scale, td.numMerged, td.useWeightLimit)
	td.unmergedIdx = td.numMerged
}

func readerString(r Reader) string {
	return fmt.Sprintf("TDigest{N=%f,(%.4f..%4f-[%.4f %.4f (%.4f) %.4f %.4f]-%.4f..%.4f}",
		r.TotalCount(),
		r.ValueAt(0),
		r.ValueAt(.1),
		r.ValueAt(.2),
		r.ValueAt(.4),
		r.InnerMean(.8),
		r.ValueAt(.6),
		r.ValueAt(.8),
		r.ValueAt(.9),
		r.ValueAt(1),
	)
}

func addTo(into Recorder, merged []tdigest.Centroid) {
	totalCount := 0.0
	for _, c := range merged {
		into.Add(c.Mean, c.Count-totalCount)
		totalCount = c.Count
	}
}

func decay(merged []tdigest.Centroid, factor float64) {
	const verySmall = 1e-9
	for i := range merged {
		if count := merged[i].Count * factor; count > verySmall {
			merged[i].Count = count
		}
	}
}
