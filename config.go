package tdigest

import "math"

type config struct {

	// compression factor controls the target maximum number of centroids in a
	// fully compressed TDigest.
	compression float64

	// bufferFactor is multiple of the compression size used to buffer unmerged
	// centroids. See config.bufferSize().
	bufferFactor int

	// scale controls how weight is apportioned to centroids.
	scale scaleFunc
}

func (cfg config) bufferSize() int {
	return int(math.Ceil(cfg.compression)) * (1 + cfg.bufferFactor)
}

// Option configures a TDigest.
type Option interface {
	apply(*config)
}

// BufferFactor configures the size of the buffer for uncompressed data.
// The default value is 5.
func BufferFactor(factor int) Option {
	return bufferFactorOption(factor)
}

func Compression(compression float64) Option {
	return compressionOption(compression)
}

type bufferFactorOption int

func (o bufferFactorOption) apply(cfg *config) { cfg.bufferFactor = int(o) }

type compressionOption float64

func (o compressionOption) apply(cfg *config) { cfg.compression = float64(o) }

type scaleOption scaleFunc

func (o scaleOption) apply(cfg *config) { cfg.scale = scaleFunc(o) }

var defaultConfig = config{
	compression:  128,
	bufferFactor: 5,
	scale:        k2,
}

type optionList []Option

func (l optionList) apply(cfg *config) {
	for _, o := range l {
		o.apply(cfg)
	}
}
