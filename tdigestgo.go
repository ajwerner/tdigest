package tdigestgo

import "sync"

type centroid struct {
	mean, count float64
}

type centroids []centroid

// TDigest approximates a distribution of floating point numbers.
type TDigest struct {
	config

	// numUnmerged is accessed with atomics
	numUnmerged int64

	centroids centroids

	mu struct {
		sync.RWMutex
		sync.Cond

		// merged is the size of the prefix of centroid used for the merged sorted
		// data.
		numMerged int
	}
}

func (td *TDigest) mergedSlice() centroids {
	return td.centroids[:td.numMerged]
}

func NewTDigest(options ...Option) *TDigest {
	var td TDigest
	defaultOptions.apply(&cfg)
	optionList(options).apply(&cfg)
	return &TDigest{

		compression: cfg.compression,
		centroids:   make(centroids, cap),
	}
}

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
