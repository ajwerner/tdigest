// Package tdigest provides a concurrent, streaming quantiles estimation data
// structure for float64 data.
package tdigest

import (
	"fmt"

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
	AddTo(Recorder)
	TotalCount() float64
	TotalSum() float64
	ValueAt(q float64) (v float64)
	QuantileOf(v float64) (q float64)
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

func (td *TDigest) TotalCount() (c float64) {
	td.compress()
	return tdigest.TotalCount(td.centroids[:td.numMerged])
}

// AddTo adds the data from td into the provided Recorder.
func (td *TDigest) AddTo(into Recorder) {
	td.compress()
	totalCount := 0.0
	for _, c := range td.centroids[:td.numMerged] {
		into.Add(c.Mean, c.Count-totalCount)
		totalCount = c.Count
	}
}

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

func (td *TDigest) TotalSum() float64 {
	td.compress()
	return tdigest.TotalSum(td.centroids[:td.numMerged])
}

func (td *TDigest) compress() {
	td.numMerged = tdigest.Compress(td.centroids[:td.unmergedIdx], td.compression, td.scale, td.numMerged, td.useWeightLimit)
	td.unmergedIdx = td.numMerged
}

func (td *TDigest) Record(mean float64) { td.Add(mean, 1) }

func readerString(r Reader) string {
	tc := r.TotalCount()
	return fmt.Sprintf("(%.4f-[%.4f %.4f %.4f]-%.4f) totalCount: %v, avg: %v",
		r.ValueAt(0),
		r.ValueAt(.25),
		r.ValueAt(.5),
		r.ValueAt(.75),
		r.ValueAt(1),
		r.TotalCount(),
		r.TotalSum()/tc)
}

func decay(merged []tdigest.Centroid, factor float64) {
	const verySmall = 1e-9
	for i := range merged {
		if count := merged[i].Count * factor; count > verySmall {
			merged[i].Count = count
		}
	}
}
